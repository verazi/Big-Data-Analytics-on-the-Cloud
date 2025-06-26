# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

import time
import requests
import argparse
import random
import calendar
from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import sys
import os

# Base API and search settings
API_BASE_URL = 'https://aus.social'
HASHTAGS = ['housing', 'affordability', 'rent', 'mortgage']
KEYWORDS = HASHTAGS
POST_INDEX = "history-mastodon-posts"
COMMENT_INDEX = "history-mastodon-comments"

# Read configuration from ConfigMap
def config(key: str) -> str:
    try:
        with open(f'/configs/default/shared-data/{key}', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        # For local development/testing
        dummy_config = {
            "MASTODON_ACCESS_TOKEN": "your_token_here",
            "ES_USERNAME": "elastic",
            "ES_PASSWORD": "elastic"
        }
        return dummy_config.get(key, "")

# HTTP headers for Mastodon API
def get_headers():
    return {
        'Authorization': f'Bearer {config("MASTODON_ACCESS_TOKEN")}',
        'Accept': 'application/json'
    }

# Initialize Elasticsearch client
def init_elasticsearch():
    return Elasticsearch(
        hosts= "https://elasticsearch-master.elastic.svc.cluster.local:9200",
        verify_certs=False,
        http_auth=(config("ES_USERNAME"), config("ES_PASSWORD"))
    )

# Disable SSL warnings
urllib3.disable_warnings(category=InsecureRequestWarning)

# Safe GET with rate-limit handling
def safe_get(url, params=None, retries=5):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=get_headers(), params=params)
            if resp.status_code == 429:
                # More robust header parsing with defaults
                wait = 60  # Default wait time
                try:
                    if 'Date' in resp.headers and 'X-RateLimit-Reset' in resp.headers:
                        req_ts = datetime.strptime(resp.headers.get('Date', ''), '%a, %d %b %Y %H:%M:%S %Z')
                        reset_ts = datetime.strptime(resp.headers.get('X-RateLimit-Reset', ''), '%Y-%m-%dT%H:%M:%S.%fZ')
                        wait = max((reset_ts - req_ts).total_seconds(), 60)
                except ValueError as e:
                    print(f"Error parsing rate limit headers: {e}. Using default wait time.")
                
                wait += random.uniform(0, 1)  
                print(f"Rate limited. Waiting {wait:.2f} seconds...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error: {e}")
            if attempt == retries - 1:
                raise
        except Exception as e:
            print(f"Error in request: {e}")
            if attempt == retries - 1:
                raise
        time.sleep(min(2 ** attempt, 60) + random.uniform(0, 1))  
    raise RuntimeError(f"Failed GET {url} after {retries} retries")

# Fetch one page of tag timeline
def fetch_tag_page(tag, max_id=None):
    url = f"{API_BASE_URL}/api/v1/timelines/tag/{tag}"
    params = {'limit': 40}
    if max_id:
        params['max_id'] = max_id
    return safe_get(url, params=params).json()

# Fetch replies for a post
def fetch_replies(post_id, state, is_historical=True):
    url = f"{API_BASE_URL}/api/v1/statuses/{post_id}/context"
    data = safe_get(url).json().get('descendants', [])
    out = []
    for r in data:
        created = date_parser.parse(r['created_at'])
        out.append({
            '_op_type': 'index',
            '_index': COMMENT_INDEX,
            '_id': r['id'],
            '_source': {
                'comment_id': r['id'],
                'post_id': post_id,
                'platform': 'mastodon',
                'author': r['account']['acct'],
                'content': r['content'],
                'created_at': created.isoformat(),
                'state': state,
                'historical_harvest': is_historical  # Add flag to comments too
            }
        })
    return out

def harvest_historical_data(tags, start_date, end_date, batch_size=100):
    es = init_elasticsearch()
    print(f"Starting historical harvest from {start_date.isoformat()} to {end_date.isoformat()} for tags: {tags}")
    
    # Track progress for all tags
    total_posts_harvested = 0
    seen_posts = set()  # Initialize seen_posts set to track duplicates across tags
    
    for tag in tags:
        print(f"Processing tag: #{tag}")
        # Initialize per-tag action list and counters
        actions = []
        posts_processed = 0
        max_id = None
        reached_start_date = False
        
        while not reached_start_date:
            statuses = fetch_tag_page(tag, max_id)
            
            if not statuses:
                print(f"No more statuses found for tag: #{tag}")
                break
                
            # Calculate the oldest post date in this page for logging
            page_oldest = date_parser.parse(statuses[-1]['created_at']) if statuses else None
                
            for st in statuses:
                sid = st['id']
                if sid in seen_posts:
                    continue
                
                created = date_parser.parse(st['created_at'])
                
                # Skip posts newer than end_date
                if created > end_date:
                    continue
                    
                # Stop when we reach posts older than start_date
                if created < start_date:
                    reached_start_date = True
                    break
                
                text = st.get('content', '') or ''
                
                # Set state as unknown - leave determination to data processing
                state = 'UNKNOWN'
                
                # Prepare post action with unique _id
                actions.append({
                    '_op_type': 'index',
                    '_index': POST_INDEX,
                    '_id': sid,
                    '_source': {
                        'post_id': sid,
                        'platform': 'mastodon',
                        'author': st['account']['acct'],
                        'content': text,
                        'created_at': created.isoformat(),
                        'tags': [tag],
                        'state': state,
                        'historical_harvest': True  
                    }
                })
                seen_posts.add(sid)
                posts_processed += 1
                
                # Pass the state and historical flag to fetch_replies
                actions.extend(fetch_replies(sid, state, is_historical=True))
        
                # If buffer full, flush
                if len(actions) >= batch_size:
                    try:
                        success, errors = helpers.bulk(
                            es, 
                            actions, 
                            chunk_size=batch_size,
                            raise_on_error=False
                        )
                        total_posts_harvested += posts_processed
                        if errors:
                            print(f"Errors in bulk operation: {errors}")
                        print(f"Indexed {posts_processed} posts ({total_posts_harvested} total)")
                        posts_processed = 0
                        actions.clear()
                    except Exception as e:
                        print(f"Error in bulk indexing: {e}")
                    time.sleep(1 + random.uniform(0, 0.5))  
            
            # Break conditions for outer while loop
            if reached_start_date or not statuses or len(statuses) < 40:
                break
                
            # If we still have more results, continue pagination
            max_id = int(statuses[-1]['id']) - 1
            print(f"Paginating with max_id={max_id}, current date: {page_oldest.isoformat()}")
            time.sleep(1 + random.uniform(0, 0.5))  
        
        # Flush any remaining actions at the end of each tag
        if actions:
            try:
                success, errors = helpers.bulk(
                    es, 
                    actions, 
                    chunk_size=batch_size,
                    raise_on_error=False
                )
                total_posts_harvested += posts_processed
                if errors:
                    print(f"Errors in bulk operation: {errors}")
                print(f"Indexed {posts_processed} posts from tag {tag} ({total_posts_harvested} total)")
            except Exception as e:
                print(f"Error in bulk indexing for tag {tag}: {e}")
            
        print(f"Finished processing tag: #{tag}")
    
    
    return total_posts_harvested

def parse_args():
    parser = argparse.ArgumentParser(description='Historical Mastodon data harvester')
    parser.add_argument('--tags', type=str, nargs='+', default=HASHTAGS, 
                      help='Tags to harvest (default: all housing-related tags)')
    parser.add_argument('--start-year', type=int, default=datetime.now().year - 5,
                      help='Start year for harvesting (default: 5 years ago)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year,
                      help='End year for harvesting (default: current year)')
    parser.add_argument('--start-month', type=int, default=1,
                      help='Start month for harvesting (default: 1)')
    parser.add_argument('--end-month', type=int, default=12,
                      help='End month for harvesting (default: 12)')
    parser.add_argument('--batch-size', type=int, default=100,
                      help='Batch size for ES bulk operations (default: 100)')
    return parser.parse_args()

def main():
    args = parse_args()
    
    start_date = datetime(args.start_year, args.start_month, 1, tzinfo=timezone.utc)
    
    # If end date is current year/month, use today's date
    if args.end_year == datetime.now().year and args.end_month == datetime.now().month:
        end_date = datetime.now(timezone.utc)
    else:
        # Simpler end date calculation using calendar
        last_day = calendar.monthrange(args.end_year, args.end_month)[1]
        end_date = datetime(args.end_year, args.end_month, last_day, 23, 59, 59, tzinfo=timezone.utc)
    
    print(f"Historical harvester starting with parameters:")
    print(f"  Tags: {args.tags}")
    print(f"  Date range: {start_date.isoformat()} to {end_date.isoformat()}")
    print(f"  Batch size: {args.batch_size}")
    
    total_posts = harvest_historical_data(args.tags, start_date, end_date, args.batch_size)
    
    print(f"Historical harvest complete. Total posts harvested: {total_posts}")

if __name__ == "__main__":
    main()
