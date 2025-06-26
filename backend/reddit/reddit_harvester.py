"""
# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)
"""

import time
import json
from datetime import datetime, timezone
from dateutil import parser as date_parser
import praw
import prawcore
import redis
from elasticsearch import Elasticsearch, helpers
import urllib3
from flask import jsonify
from typing import Optional, Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def config(key: str) -> str:
    with open(f'/configs/default/shared-data-reddit/{key}', 'r') as f:
        return f.read().strip()

SUBREDDITS = ['Australia', 'Melbourne', 'housing', 'AusFinance', 'Sydney', 'Brisbane', 'Perth', 'Adelaide']
KEYWORDS = ['housing', 'house prices', 'affordability', 'rent', 'mortgage', 'rental', 'property']
USER_AGENT = 'HousingSentimentBot/0.1 by u/Emotional-Exam-3670'

urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)

class ESManager:
    def __init__(self):
        # Initialize Elasticsearch client with authentication and configuration
        self.es = Elasticsearch(
            hosts="https://elasticsearch-master.elastic.svc.cluster.local:9200",
            verify_certs=False,
            http_auth=(config("ES_USERNAME"), config("ES_PASSWORD"))
        )
    
    def bulk_check_exists(self, index: str, ids: List[str]) -> set:
        # Check if documents with given IDs exist in the specified Elasticsearch index
        if not ids:
            return set()
        try:
            result = self.es.mget(index=index, body={"ids": ids}, _source=False)
            return {doc['_id'] for doc in result.get('docs', []) if doc.get('found', False)}
        except Exception:
            return set()
    
    def get_last_timestamp(self, subreddit: str, keyword: str) -> Optional[datetime]:
        # Retrieve the last processed timestamp for a subreddit and keyword from Elasticsearch
        try:
            control_id = f"last_timestamp_{subreddit}_{keyword}"
            if self.es.exists(index="reddit-control", id=control_id):
                result = self.es.get(index="reddit-control", id=control_id)
                return date_parser.parse(result["_source"]["last_timestamp"])
        except Exception:
            pass
        return None
    
    def update_timestamp(self, subreddit: str, keyword: str, timestamp: datetime):
        # Update the last processed timestamp for a subreddit and keyword in Elasticsearch
        try:
            control_id = f"last_timestamp_{subreddit}_{keyword}"
            self.es.index(
                index="reddit-control",
                id=control_id,
                document={
                    "subreddit": subreddit,
                    "keyword": keyword,
                    "last_timestamp": timestamp.isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
            )
        except Exception as e:
            logger.error(f"Failed to update timestamp: {e}")
    
    def bulk_index(self, actions: List[Dict]):
        # Perform bulk indexing of documents into Elasticsearch
        try:
            for action in actions:
                action['_op_type'] = 'create'
            
            success, errors = helpers.bulk(self.es, actions, chunk_size=50, raise_on_error=False, timeout='30s')
            
            duplicate_count = sum(1 for error in errors if 'already_exists' in str(error) or 'version_conflict' in str(error))
            if duplicate_count > 0:
                logger.info(f"Indexed {success} new, {duplicate_count} duplicates skipped")
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")

class RedisStreamManager:
    def __init__(self):
        # Initialize Redis client and create consumer group for task management
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
        self.consumer_group = "reddit_harvesters"
        self.consumer_name = "reddit_harvester_main"
        
        try:
            self.redis_client.xgroup_create(self.stream_name, self.consumer_group, id='0', mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"Failed to create consumer group: {e}")
    
    def consume_task(self) -> Optional[Dict]:
        # Consume a task from the Redis stream
        try:
            self._claim_abandoned_messages()
            
            messages = self.redis_client.xreadgroup(
                self.consumer_group, self.consumer_name,
                {self.stream_name: '>'}, count=1, block=1000
            )
            
            if messages and messages[0][1]:
                message_id, fields = messages[0][1][0]
                task = json.loads(fields['task_data'])
                task['message_id'] = message_id
                logger.info(f"Consumed task {task.get('task_id', '')}")
                return task
            return None
        except Exception as e:
            logger.error(f"Failed to consume: {e}")
            return None
    
    def _claim_abandoned_messages(self):
        # Claim abandoned messages in the Redis stream for reprocessing
        try:
            pending = self.redis_client.xpending_range(self.stream_name, self.consumer_group, min='-', max='+', count=10)
            for msg in pending:
                message_id, consumer, idle_time, delivery_count = msg
                if idle_time > 60000:  
                    try:
                        claimed = self.redis_client.xclaim(self.stream_name, self.consumer_group, self.consumer_name, 60000, [message_id])
                        if claimed:
                            logger.info(f"Claimed abandoned message {message_id} from consumer {consumer} (idle: {idle_time/1000:.1f}s)")
                    except Exception as e:
                        logger.warning(f"Failed to claim message {message_id}: {e}")
        except Exception as e:
            logger.warning(f"Failed to check pending messages: {e}")
    
    def complete_task(self, task: Dict, success: bool = True):
        # Acknowledge the completion of a task in the Redis stream
        try:
            message_id = task.get('message_id')
            if message_id:
                self.redis_client.xack(self.stream_name, self.consumer_group, message_id)
                logger.info(f"Task {task.get('task_id', '')} {'completed' if success else 'failed'}")
        except Exception as e:
            logger.error(f"Failed to acknowledge: {e}")

class RedditHarvester:
    def __init__(self):
        # Initialize Reddit API client, Elasticsearch manager, and Redis stream manager
        self.reddit = praw.Reddit(
            client_id=config('REDDIT_CLIENT_ID'),
            client_secret=config('REDDIT_CLIENT_SECRET'),
            user_agent=USER_AGENT
        )
        self.es_manager = ESManager()
        self.queue_manager = RedisStreamManager()

    def reddit_safe_call(self, func, *args, retries=5, **kwargs):
        # Safely call a Reddit API function with retry logic for rate limits and errors
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except prawcore.TooManyRequests as e:
                wait = int(e.response.headers.get('Retry-After', 60))
                logger.warning(f"Reddit 429, sleeping {wait}s")
                time.sleep(wait)
            except Exception:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        raise RuntimeError(f"Reddit call failed: {func.__name__}")

    def process_task(self, task: Dict) -> Dict:
        # Process a task to collect and index Reddit posts and comments based on the task parameters
        subreddit = task['subreddit']
        keyword = task['keyword']
        max_posts = task.get('max_posts', 50)
        start_time = date_parser.parse(task['start_time'])
        end_time = date_parser.parse(task.get('end_time', task['start_time'])) 
        
        logger.info(f"Processing: {subreddit} - {keyword} (Historical Mode)")
        logger.info(f"Task time window: {start_time.isoformat()} to {end_time.isoformat()}")
        
        # Get the last processed timestamp (this represents the earliest time we've processed so far)
        last_time = self.es_manager.get_last_timestamp(subreddit, keyword)
        
        if last_time is None:
            # First run: start from end_time (most recent) and work backwards to start_time
            effective_end = end_time
            effective_start = start_time
            logger.info(f"First historical run: processing {effective_start.isoformat()} to {effective_end.isoformat()}")
        else:
            # Continue historical collection: last_time is the earliest we've processed
            # New window should be from start_time to last_time (going further back)
            effective_end = last_time
            effective_start = start_time
            logger.info(f"Continuing historical collection: processing {effective_start.isoformat()} to {effective_end.isoformat()}")
        
        try:
            # Calculate time filter based on the historical range
            days_back = (datetime.now(timezone.utc) - effective_start).days
            time_filter = 'day' if days_back <= 1 else 'week' if days_back <= 7 else 'month' if days_back <= 30 else 'year' if days_back <= 365 else 'all'
            
            logger.info(f"Using time_filter: {time_filter} for {days_back} days back")
            
            # Get and sort submissions
            submissions = self.reddit_safe_call(
                self.reddit.subreddit(subreddit).search,
                keyword, time_filter=time_filter, limit=max_posts, sort='new'
            )
            # Sort by creation time descending (newest first) for historical processing
            submission_list = sorted(list(submissions), key=lambda x: x.created_utc, reverse=True)
            
            # Collect IDs for bulk check
            post_ids = []
            comment_ids = []
            submission_data = []
            
            for submission in submission_list:
                created = datetime.fromtimestamp(submission.created_utc, timezone.utc)
                
                # HISTORICAL MODE: Only process posts within our historical window
                # Skip posts that are too new (> effective_end) or too old (< effective_start)
                if created > effective_end:
                    continue  # Too new, skip
                if created < effective_start:
                    continue  # Too old, skip
                
                post_ids.append(submission.id)
                submission_data.append((submission, created))
                
                try:
                    submission.comments.replace_more(limit=3)
                    comment_ids.extend([c.id for c in submission.comments.list()[:50]])
                except Exception:
                    pass
            
            # Bulk check existing documents
            existing_posts = self.es_manager.bulk_check_exists('reddit-posts', post_ids)
            existing_comments = self.es_manager.bulk_check_exists('reddit-comments', comment_ids)
            
            # Process new posts and comments
            actions = []
            posts_processed = 0
            earliest_time = effective_end  # Track the earliest time we've processed (for historical progression)
            
            for submission, created in submission_data:
                if submission.id in existing_posts:
                    continue
                
                # For historical mode, track the earliest timestamp we've processed
                if created < earliest_time:
                    earliest_time = created
                
                content = submission.title + (" " + submission.selftext if submission.selftext else "")
                
                actions.append({
                    '_index': 'reddit-posts',
                    '_id': submission.id,
                    '_source': {
                        'post_id': submission.id,
                        'platform': 'reddit',
                        'author': str(submission.author) if submission.author else '[deleted]',
                        'content': content,
                        'created_at': created.isoformat(),
                        'tags': [subreddit, keyword],
                        'subreddit': subreddit,
                        'keyword': keyword,
                        'score': submission.score,
                        'num_comments': submission.num_comments,
                        'task_id': task.get('task_id', ''),
                        'collection_mode': 'historical'
                    }
                })
                posts_processed += 1
                
                # Add new comments
                try:
                    submission.comments.replace_more(limit=2)
                    for comment in submission.comments.list():
                        if comment.id not in existing_comments:
                            actions.append({
                                '_index': 'reddit-comments',
                                '_id': comment.id,
                                '_source': {
                                    'comment_id': comment.id,
                                    'post_id': submission.id,
                                    'platform': 'reddit',
                                    'author': str(comment.author) if comment.author else '[deleted]',
                                    'content': comment.body,
                                    'created_at': datetime.fromtimestamp(comment.created_utc, timezone.utc).isoformat(),
                                    'collection_mode': 'historical'
                                }
                            })
                except Exception:
                    pass
                
                if len(actions) >= 100:
                    self.es_manager.bulk_index(actions)
                    actions.clear()
            
            if actions:
                self.es_manager.bulk_index(actions)
            
            # HISTORICAL MODE: Update timestamp to the earliest time we've processed
            # This ensures next run will continue from where we left off (going further back in time)
            if posts_processed > 0 and earliest_time < effective_end:
                self.es_manager.update_timestamp(subreddit, keyword, earliest_time)
                logger.info(f"Updated timestamp to {earliest_time.isoformat()} (historical progression towards 2019)")
            elif posts_processed == 0:
                logger.info(f"No new posts found in window {effective_start.isoformat()} to {effective_end.isoformat()}")
            
            return {
                'status': 'success',
                'task_id': task.get('task_id', ''),
                'subreddit': subreddit,
                'keyword': keyword,
                'posts_processed': posts_processed,
                'time_window': f"{effective_start.isoformat()} to {effective_end.isoformat()}",
                'earliest_processed': earliest_time.isoformat() if posts_processed > 0 else None,
                'collection_mode': 'historical',
                'completed_at': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Task failed {task.get('task_id', '')}: {e}")
            return {
                'status': 'error',
                'task_id': task.get('task_id', ''),
                'error': str(e),
                'failed_at': datetime.now(timezone.utc).isoformat()
            }

class HarvestController:
    def __init__(self):
        # Initialise the Reddit harvester
        self.harvester = RedditHarvester()
    
    def handle_request(self):
        # Handle multiple tasks in a single execution for higher throughput
        try:
            tasks_processed = 0
            max_tasks_per_run = 2  
            results = []
            
            while tasks_processed < max_tasks_per_run:
                task = self.harvester.queue_manager.consume_task()
                
                if not task:
                    break  # No more tasks available
                
                result = self.harvester.process_task(task)
                success = result['status'] == 'success'
                
                # Only ACK messages when successful to prevent message loss
                if success:
                    self.harvester.queue_manager.complete_task(task, success)
                    logger.info(f"Task {task.get('task_id', '')} completed successfully and ACKed")
                else:
                    # Failed tasks are not ACKed, allowing Redis Stream to retry
                    logger.warning(f"Task {task.get('task_id', '')} failed, not ACKing - will be retried")
                
                results.append(result)
                tasks_processed += 1
                
                # If a task fails, stop processing more tasks (to avoid cascading failures)
                if not success:
                    logger.info(f"Stopping batch processing due to failed task")
                    break
            
            if tasks_processed == 0:
                return jsonify({
                    'status': 'no_tasks',
                    'message': 'No tasks available',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }), 200
            
            # Calculate successful and failed tasks
            successful_tasks = sum(1 for r in results if r['status'] == 'success')
            failed_tasks = tasks_processed - successful_tasks
            
            # Return the batch processing result
            return jsonify({
                'status': 'batch_success',
                'tasks_processed': tasks_processed,
                'successful_tasks': successful_tasks,
                'failed_tasks': failed_tasks,
                'results': results,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }), 200
                
        except Exception as e:
            logger.error(f"Handler error: {e}")
            return jsonify({
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }), 500

def main():
    # Fission function entry point
    return HarvestController().handle_request()
