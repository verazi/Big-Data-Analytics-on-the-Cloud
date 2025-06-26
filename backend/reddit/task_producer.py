"""
# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)
"""

import json
import redis
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from ConfigMap
def config(key: str) -> str:
    with open(f'/configs/default/shared-data-reddit/{key}', 'r') as f:
            return f.read().strip()

# Reddit configuration
SUBREDDITS = ['Australia', 'Melbourne', 'housing', 'AusFinance', 'sydney', 'brisbane', 'perth', 'adelaide']
KEYWORDS = ['housing', 'house prices', 'affordability', 'rent', 'mortgage', 'rental', 'property']

class TaskProducer:
    def __init__(self):
        redis_config = {
            'host': config('REDIS_HOST') or 'redis-master.redis.svc.cluster.local',
            'port': int(config('REDIS_PORT') or '6379'),
            'decode_responses': True,
            'socket_connect_timeout': 5,
            'socket_timeout': 5
        }
        
        redis_password = config('REDIS_PASSWORD') or ''
        if redis_password:
            redis_config['password'] = redis_password
        
        self.redis_client = redis.Redis(**redis_config)
        self.redis_client.ping()
        
        self.stream_name = "reddit_harvest_stream"
        self.last_generation_key = "reddit_task_producer_last_generation"
    
    def get_last_generation_time(self) -> Optional[datetime]:
        """Get the last task generation timestamp"""
        try:
            last_time_str = self.redis_client.get(self.last_generation_key)
            if last_time_str:
                return datetime.fromisoformat(last_time_str)
        except Exception as e:
            logger.error(f"Failed to get last generation time: {e}")
        return None
    
    def update_last_generation_time(self, timestamp: datetime):
        """Update the last task generation timestamp"""
        try:
            self.redis_client.set(
                self.last_generation_key, 
                timestamp.isoformat(),
                ex=7*24*3600  # Keep for 7 days
            )
        except Exception as e:
            logger.error(f"Failed to update last generation time: {e}")
    
    def calculate_time_window(self, default_lookback_hours: int = 6) -> tuple[datetime, datetime]:
        """Calculate the time window for task generation to avoid overlaps"""
        now = datetime.now(timezone.utc)
        last_generation = self.get_last_generation_time()
        
        if last_generation is None:
            # First run - use default lookback
            start_time = now - timedelta(hours=default_lookback_hours)
            logger.info(f"First run detected, using {default_lookback_hours}h lookback")
        else:
            # Incremental run - only generate tasks for new time window
            # Add small buffer (5 minutes) to ensure we don't miss anything
            start_time = last_generation - timedelta(minutes=5)
            time_diff = (now - last_generation).total_seconds() / 3600
            logger.info(f"Incremental run, generating tasks for {time_diff:.1f}h window")
        
        return start_time, now
    
    def calculate_historical_time_window(self, window_weeks: int = 4) -> tuple[datetime, datetime]:
        """Calculate time window for historical data collection (continuous historical collection)"""
        now = datetime.now(timezone.utc)
        last_generation = self.get_last_generation_time()
        
        # Historical collection: start from recent and go backwards
        if last_generation is None:
            # First run - start from now and go back a few weeks
            end_time = now
            start_time = now - timedelta(weeks=window_weeks)
            logger.info(f"First historical run: {start_time.isoformat()} to {end_time.isoformat()}")
        else:
            # Continue backwards from last generation point
            end_time = last_generation
            start_time = last_generation - timedelta(weeks=window_weeks)  
            
            logger.info(f"Historical continuation: {start_time.isoformat()} to {end_time.isoformat()}")
        
        return start_time, end_time
    
    def generate_harvest_tasks(self, default_lookback_hours: int = 6) -> List[Dict]:
        """Generate harvest tasks for the message queue with smart time windowing"""
        start_time, end_time = self.calculate_time_window(default_lookback_hours)
        
        # Skip if time window is too small 
        time_diff_minutes = (end_time - start_time).total_seconds() / 60
        if time_diff_minutes < 5:  
            logger.info(f"Time window too small ({time_diff_minutes:.1f}min), skipping generation")
            return []
        
        tasks = []
        
        # Generate tasks for each subreddit-keyword combination
        for subreddit in SUBREDDITS:
            for keyword in KEYWORDS:
                task = {
                    'task_id': f"{subreddit}_{keyword}_{int(end_time.timestamp())}",
                    'subreddit': subreddit,
                    'keyword': keyword,
                    'max_posts': 200,  
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),  
                    'created_at': end_time.isoformat(),
                    'priority': self._get_priority(subreddit, keyword),
                    'retry_count': 0,
                    'max_retries': 3,
                    'time_window_minutes': int(time_diff_minutes)
                }
                tasks.append(task)
        
        logger.info(f"Generated {len(tasks)} tasks for window: {start_time.isoformat()} to {end_time.isoformat()}")
        return tasks
    
    def generate_historical_harvest_tasks(self, window_weeks: int = 4) -> List[Dict]:
        """Generate historical harvest tasks going backwards in time (continuous historical collection)"""
        start_time, end_time = self.calculate_historical_time_window(window_weeks=window_weeks)
        
        # Skip if time window is too small 
        time_diff_days = (end_time - start_time).days
        if time_diff_days < 1:  
            logger.info(f"Time window too small ({time_diff_days} days), skipping generation")
            return []
        
        tasks = []
        
        # Generate tasks for each subreddit-keyword combination
        for subreddit in SUBREDDITS:
            for keyword in KEYWORDS:
                task = {
                    'task_id': f"historical_{subreddit}_{keyword}_{int(end_time.timestamp())}",
                    'subreddit': subreddit,
                    'keyword': keyword,
                    'max_posts': 300,  
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'created_at': datetime.now(timezone.utc).isoformat(),
                    'priority': self._get_priority(subreddit, keyword),
                    'retry_count': 0,
                    'max_retries': 3,
                    'mode': 'historical',
                    'time_window_days': time_diff_days
                }
                tasks.append(task)
        
        logger.info(f"Generated {len(tasks)} historical tasks for window: {start_time.isoformat()} to {end_time.isoformat()} ({time_diff_days} days)")
        return tasks
    
    def _get_priority(self, subreddit: str, keyword: str) -> int:
        """Assign priority to tasks based on subreddit and keyword importance"""
        high_priority_subreddits = ['Melbourne', 'housing', 'AusFinance']
        high_priority_keywords = ['housing', 'house prices', 'affordability']
        
        if subreddit in high_priority_subreddits and keyword in high_priority_keywords:
            return 1  # High priority
        elif subreddit in high_priority_subreddits or keyword in high_priority_keywords:
            return 2  # Medium priority
        else:
            return 3  # Low priority
    
    def queue_tasks(self, tasks: List[Dict]) -> int:
        """Queue tasks to Redis Stream"""
        if not tasks:
            logger.info("No tasks to queue")
            return 0
            
        queued_count = 0
        
        # Sort by priority - high priority tasks go first
        tasks.sort(key=lambda x: x['priority'])
        
        for task in tasks:
            try:
                # Add task to stream with task_data field expected by harvester
                message_id = self.redis_client.xadd(
                    self.stream_name,
                    {'task_data': json.dumps(task)},
                    maxlen=10000  # Keep last 10k messages
                )
                queued_count += 1
                logger.debug(f"Queued task {task['task_id']} to stream with ID {message_id}")
            except Exception as e:
                logger.error(f"Failed to queue task {task['task_id']}: {e}")
        
        # Update last generation time only after successful queuing
        if queued_count > 0:
            # Use the end_time from the first task (they should all have the same end_time)
            end_time = datetime.fromisoformat(tasks[0]['end_time'])
            self.update_last_generation_time(end_time)
        
        logger.info(f"Queued {queued_count} tasks to Redis Stream")
        return queued_count
    
    def get_stream_status(self) -> Dict:
        """Get current stream and consumer group status"""
        try:
            # Get stream info
            stream_info = self.redis_client.xinfo_stream(self.stream_name)
            stream_length = stream_info.get('length', 0)
            
            # Get consumer group info
            consumer_groups = self.redis_client.xinfo_groups(self.stream_name)
            
            status = {
                'stream_length': stream_length,
                'consumer_groups': [],
                'last_generation_time': self.get_last_generation_time()
            }
            
            for group in consumer_groups:
                group_name = group['name']
                pending_count = group['pending']
                
                # Get detailed pending info
                try:
                    pending_summary = self.redis_client.xpending(self.stream_name, group_name)
                    if pending_summary:
                        pending_count = pending_summary['pending']
                except Exception:
                    pass
                
                status['consumer_groups'].append({
                    'name': group_name,
                    'pending': pending_count,
                    'consumers': group.get('consumers', 0)
                })
            
            return status
        except Exception as e:
            logger.error(f"Failed to get stream status: {e}")
            return {'stream_length': 0, 'consumer_groups': [], 'last_generation_time': None}
    
    def cleanup_processed_messages(self, max_age_hours: int = 1):
        """Clean up old processed messages from stream - More aggressive cleanup"""
        try:
            stream_info = self.redis_client.xinfo_stream(self.stream_name)
            current_length = stream_info.get('length', 0)
            
            # More aggressive cleanup strategy to prevent stream buildup
            if current_length > 200:  
                self.redis_client.xtrim(self.stream_name, maxlen=50, approximate=True)
                logger.info(f"Trimmed stream from {current_length} to ~50 messages")
            elif current_length > 100:
                # Medium cleanup: keep last 30 messages
                self.redis_client.xtrim(self.stream_name, maxlen=30, approximate=True)
                logger.info(f"Trimmed stream from {current_length} to ~30 messages")
                
        except Exception as e:
            logger.error(f"Failed to cleanup messages: {e}")
    
    def get_effective_queue_size(self) -> Dict:
        """Get effective queue size based on pending tasks rather than total stream length"""
        try:
            status = self.get_stream_status()
            total_pending = 0
            total_lag = 0
            
            for group in status.get('consumer_groups', []):
                total_pending += group.get('pending', 0)
                # Calculate lag from stream info if available
                try:
                    group_info = self.redis_client.xinfo_groups(self.stream_name)
                    for g in group_info:
                        if g['name'] == group['name']:
                            # Lag represents unprocessed messages
                            total_lag += g.get('lag', 0)
                except Exception:
                    pass
            
            return {
                'stream_length': status.get('stream_length', 0),
                'pending_tasks': total_pending,
                'lag': total_lag,
                'effective_queue_size': total_pending + total_lag
            }
        except Exception as e:
            logger.error(f"Failed to get effective queue size: {e}")
            return {'stream_length': 0, 'pending_tasks': 0, 'lag': 0, 'effective_queue_size': 0}
    
    def requeue_failed_tasks(self):
        """Handle failed tasks using Redis Streams pending messages"""
        try:
            # Get consumer groups
            consumer_groups = self.redis_client.xinfo_groups(self.stream_name)
            requeued_count = 0
            
            for group in consumer_groups:
                group_name = group['name']
                
                # Get pending messages for this group
                try:
                    pending_messages = self.redis_client.xpending_range(
                        self.stream_name, 
                        group_name, 
                        min='-', 
                        max='+', 
                        count=100
                    )
                    
                    for msg in pending_messages:
                        message_id, consumer, idle_time, delivery_count = msg
                        
                        # If message has been idle for more than 10 minutes
                        if idle_time > 600000:  
                            try:
                                # Get the message content
                                messages = self.redis_client.xrange(self.stream_name, message_id, message_id)
                                if messages:
                                    _, fields = messages[0]
                                    task_data = json.loads(fields['task_data'])
                                    
                                    # Check retry count
                                    retry_count = task_data.get('retry_count', 0)
                                    max_retries = task_data.get('max_retries', 3)
                                    
                                    if retry_count < max_retries:
                                        # Increment retry count and requeue
                                        task_data['retry_count'] = retry_count + 1
                                        task_data['requeued_at'] = datetime.now(timezone.utc).isoformat()
                                        
                                        # Add back to stream
                                        self.redis_client.xadd(
                                            self.stream_name,
                                            {'task_data': json.dumps(task_data)}
                                        )
                                        requeued_count += 1
                                        logger.warning(f"Requeued failed task: {task_data['task_id']}, retry {task_data['retry_count']}")
                                    else:
                                        logger.error(f"Task exceeded max retries: {task_data['task_id']}")
                                    
                                    
                            except Exception as e:
                                logger.error(f"Failed to process pending message {message_id}: {e}")
                
                except Exception as e:
                    logger.error(f"Failed to get pending messages for group {group_name}: {e}")
            
            return requeued_count
            
        except Exception as e:
            logger.error(f"Failed to requeue failed tasks: {e}")
            return 0
    
    def clear_stream(self):
        """Clear the stream (for maintenance)"""
        try:
            # Delete the entire stream and last generation time
            self.redis_client.delete(self.stream_name)
            self.redis_client.delete(self.last_generation_key)
            logger.info("Stream and generation tracking cleared")
        except Exception as e:
            logger.error(f"Failed to clear stream: {e}")

def main():
    """Main function for K8s Job - Historical Mode (2025 -> 2019) - Improved Logic"""
    try:
        producer = TaskProducer()
        
        # Step 1: Aggressive cleanup of old messages first
        producer.cleanup_processed_messages(max_age_hours=1)
        
        # Step 2: Requeue any failed tasks
        requeued_count = producer.requeue_failed_tasks()
        if requeued_count > 0:
            logger.info(f"Requeued {requeued_count} failed tasks")
        
        # Step 3: Get effective queue status (pending + lag, not total stream length)
        queue_info = producer.get_effective_queue_size()
        logger.info(f"Current queue status: {queue_info}")
        
        # Step 4: Improved decision logic - use pending tasks and lag instead of stream length
        should_generate_tasks = True
        effective_queue_size = queue_info.get('effective_queue_size', 0)
        pending_tasks = queue_info.get('pending_tasks', 0)
        stream_length = queue_info.get('stream_length', 0)
        
        # Thresholds:
        # - If too many pending tasks, wait
        # - If effective queue (pending + lag) is too large, wait
        # - Only skip if there are actually unprocessed tasks
        if pending_tasks > 30:
            should_generate_tasks = False
            logger.info(f"Too many pending tasks ({pending_tasks}), skipping generation")
        elif effective_queue_size > 50:
            should_generate_tasks = False
            logger.info(f"Effective queue size too large ({effective_queue_size}), skipping generation")
        
        # Step 5: Generate new tasks if queue is not overloaded
        if should_generate_tasks:
            logger.info("Running in HISTORICAL MODE - continuous historical data collection")
            tasks = producer.generate_historical_harvest_tasks(window_weeks=2)
            
            if tasks:
                queued_count = producer.queue_tasks(tasks)
                if queued_count > 0:
                    logger.info(f"Generated and queued {queued_count} historical tasks")
                    
                    # Update last generation time for historical progression
                    start_time = datetime.fromisoformat(tasks[0]['start_time'])
                    producer.update_last_generation_time(start_time)
                    logger.info(f"Updated historical progression to {start_time.isoformat()}")
                else:
                    logger.info("Failed to queue historical tasks")
            else:
                logger.info("No historical tasks generated (time window too small or reached end)")
        
        # Step 6: Force cleanup if stream becomes too large regardless of other conditions
        if stream_length > 300:
            logger.info(f"Stream length ({stream_length}) too large, forcing emergency cleanup")
            try:
                producer.redis_client.xtrim(producer.stream_name, maxlen=50, approximate=True)
                logger.info("Emergency stream cleanup completed")
            except Exception as e:
                logger.error(f"Failed to perform emergency cleanup: {e}")
        
        # Step 7: Final status report
        final_queue_info = producer.get_effective_queue_size()
        logger.info(f"Final queue status: {final_queue_info}")
        
    except Exception as e:
        logger.error(f"Error in historical task producer: {e}")
        raise

if __name__ == '__main__':
    main()
