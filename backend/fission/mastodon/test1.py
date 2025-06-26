# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

import time
import requests
from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from flask import jsonify
from typing import Optional, Dict, List, Tuple
import re
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Base API and search settings
API_BASE_URL = 'https://aus.social'
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']
HASHTAGS = ['housing', 'affordability', 'rent', 'mortgage']
KEYWORDS = HASHTAGS
# Default fallback if no last run exists - only get posts from the last 24 hours
DEFAULT_START_DATE = datetime.now(timezone.utc) - timedelta(days=1)

# Read configuration from ConfigMap
def config(key: str) -> str:
    with open(f'/configs/default/shared-data/{key}', 'r') as f:
        return f.read().strip()

# HTTP headers for Mastodon API
def get_headers():
    return {
        'Authorization': f'Bearer {config("MASTODON_ACCESS_TOKEN")}',
        'Accept': 'application/json'
    }

# Initialize Elasticsearch client globally
es = Elasticsearch(
    hosts= "https://elasticsearch-master.elastic.svc.cluster.local:9200",
    verify_certs=False,
    http_auth=(config("ES_USERNAME"), config("ES_PASSWORD"))
)

# Disable SSL warnings
urllib3.disable_warnings(category=InsecureRequestWarning)

# Get the timestamp of the last successful run and since_ids for tags
def get_last_run_data() -> Tuple[datetime, Dict[str, str]]:
    """Retrieve the last successful run timestamp and since_ids from Elasticsearch"""
    since_ids = {}
    try:
        # Check if the control index exists
        if not es.indices.exists(index="housing-control"):
            print("Control index doesn't exist. Using default start date.")
            return DEFAULT_START_DATE, since_ids

        # Check if the control document exists
        if es.exists(index="housing-control", id="last_run"):
            result = es.get(index="housing-control", id="last_run")
            last_run_time = date_parser.parse(result["_source"]["timestamp"])
        else:
            last_run_time = DEFAULT_START_DATE

        # Get since_ids for all tags
        for tag in HASHTAGS:
            tag_id = f"since_id_{tag}"
            if es.exists(index="housing-control", id=tag_id):
                result = es.get(index="housing-control", id=tag_id)
                since_ids[tag] = result["_source"]["since_id"]
    except Exception as e:
        print(f"Error retrieving last run data: {e}")
        return DEFAULT_START_DATE, since_ids
        
    return last_run_time, since_ids

# Update the timestamp and since_ids after successful run
def update_last_run_data(tag_since_ids: Dict[str, int]):
    """Update the last successful run timestamp and tag since_ids in Elasticsearch"""
    now = datetime.now(timezone.utc)
    
    # Update last run timestamp
    es.index(
        index="housing-control",
        id="last_run",
        document={"timestamp": now.isoformat()}
    )
    
    # Update since_ids for each tag
    for tag, since_id in tag_since_ids.items():
        if since_id:
            es.index(
                index="housing-control",
                id=f"since_id_{tag}",
                document={"tag": tag, "since_id": since_id}
            )
    
    return now

# Safe GET with rate-limit handling
def safe_get(url: str, params: Optional[Dict] = None, retries: int = 5) -> requests.Response:
    for _ in range(retries):
        resp = requests.get(url, headers=get_headers(), params=params)
        if resp.status_code == 429:
            req_ts = datetime.strptime(resp.headers.get('Date'), '%a, %d %b %Y %H:%M:%S %Z')
            reset_ts = datetime.strptime(resp.headers.get('X-RateLimit-Reset'), '%Y-%m-%dT%H:%M:%S.%fZ')
            wait = max((reset_ts - req_ts).total_seconds(), 60)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Failed GET {url} after {retries} retries")

# Fetch one page of tag timeline
def fetch_tag_page(tag: str, since_id: Optional[str] = None, max_id: Optional[int] = None) -> List[Dict]:
    url = f"{API_BASE_URL}/api/v1/timelines/tag/{tag}"
    params = {'limit': 40}
    if since_id:
        params['since_id'] = since_id
    if max_id:
        params['max_id'] = max_id
    return safe_get(url, params=params).json()

# Clean data
def clean_content(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    for a in soup.find_all("a"):
        a.replace_with(a.get_text())
    cleaned = soup.get_text(separator=" ")
    cleaned = re.sub(r'https?://\S+', '', cleaned)
    cleaned = re.sub(r'[^#\w\s]', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned.lower()

# Sentiment score
analyzer = SentimentIntensityAnalyzer()
def analyze_sentiment(text: str) -> Tuple[float, str]:
     vs = analyzer.polarity_scores(text)
     score = vs["compound"]
     label = "positive" if score >= 0.05 else ("negative" if score <= -0.05 else "neutral")
     return score, label

# Fetch replies for a post
def fetch_replies(post_id: str, state: str) -> List[Dict]:
    url = f"{API_BASE_URL}/api/v1/statuses/{post_id}/context"
    data = safe_get(url).json().get('descendants', [])
    out = []
    for r in data:
        created = date_parser.parse(r['created_at'])
        cleaned_content = clean_content(r['content'])
        # score_r = 0.0
        # sentiment_r = 'neutral'
        score_r, sentiment_r = analyze_sentiment(cleaned_content)
        out.append({
            '_op_type': 'index',
            '_index': comment_index,
            '_id': r['id'],
            '_source': {
                'comment_id': r['id'],
                'post_id': post_id,
                'platform': 'mastodon',
                'author': r['account']['acct'],
                'content': cleaned_content,
                'created_at': created.isoformat(),
                'state': state,  # Add state to comments
                'sentiment': sentiment_r,
                'sentiment_score': score_r
            }
        })
    return out

# Main entrypoint for Fission
def main():
    post_index = "housing-posts"
    global comment_index
    comment_index = "housing-comments"

    # Get the timestamp and since_ids of the last successful run
    last_run_time, since_ids = get_last_run_data()
    print(f"Fetching posts since: {last_run_time.isoformat()}")
    print(f"Using since_ids: {since_ids}")

    # Bulk action buffer
    actions = []
    BULK_SIZE = 100  # Number of docs per bulk request
    posts_processed = 0  # Track number of posts processed
    seen_posts = set()  # Track seen posts across all states and tags
    
    # Track latest ID for each tag
    latest_since_ids = {tag: since_ids.get(tag, "0") for tag in HASHTAGS}

    try:
        for tag in HASHTAGS:
                since_id = since_ids.get(tag)
                max_id = None
                
                while True:
                    statuses = fetch_tag_page(tag, since_id, max_id)
                    if not statuses:
                        break
                    
                    # Update the latest since_id for this tag
                    if statuses and int(statuses[0]['id']) > int(latest_since_ids[tag] or "0"):
                        latest_since_ids[tag] = statuses[0]['id']

                    found_old_post = False
                    for st in statuses:
                        sid = st['id']
                        if sid in seen_posts:
                            continue
                        
                        created = date_parser.parse(st['created_at'])
                        # Skip posts older than our last run (safety check)
                        if created <= last_run_time:
                            found_old_post = True
                            continue

                        text = st.get('content', '') or ''
                        if any(kw in text.lower() for kw in KEYWORDS):
                            # Set state as unknown - leave determination to data processing
                            state = 'UNKNOWN'
                            text = clean_content(text)
                            # score = 0.0
                            # sentiment = 'neutral'
                            score, sentiment = analyze_sentiment(text)
                            
                            # prepare post action with unique _id
                            actions.append({
                                '_op_type': 'index',
                                '_index': post_index,
                                '_id': sid,
                                '_source': {
                                    'post_id': sid,
                                    'platform': 'mastodon',
                                    'author': st['account']['acct'],
                                    'content': text,
                                    'created_at': created.isoformat(),
                                    'tags': [tag],
                                    'state': state,
                                    'sentiment': sentiment,
                                    'sentiment_score': score
                                }
                            })
                            seen_posts.add(sid)
                            posts_processed += 1

                            # Pass the state to fetch_replies
                            actions.extend(fetch_replies(sid, state))

                            # If buffer full, flush
                            if len(actions) >= BULK_SIZE:
                                success, errors = helpers.bulk(
                                    es, 
                                    actions, 
                                    chunk_size=BULK_SIZE,
                                    raise_on_error=False
                                )
                                if errors:
                                    print(f"Errors in bulk operation: {errors}")
                                actions.clear()
                                time.sleep(1)  # throttle to avoid overwhelming ES

                    # Stop paginating when we hit posts older than last_run_time
                    if found_old_post or not statuses or len(statuses) < 40:
                        break
                    
                    max_id = int(statuses[-1]['id']) - 1
                    time.sleep(1)  # Be nice to the API  

        # Flush any remaining actions
        if actions:
            success, errors = helpers.bulk(
                es, 
                actions, 
                chunk_size=BULK_SIZE,
                raise_on_error=False
            )
            if errors:
                print(f"Errors in final bulk operation: {errors}")
        
        # Update the since_ids after successful completion
        update_last_run_data(latest_since_ids)
        
        return jsonify({
            'message': f'Incremental harvest complete. Processed {posts_processed} new posts.',
            'last_run': datetime.now(timezone.utc).isoformat()
        }), 200
        
    except Exception as e:
        # If there's an error, don't update the last run time
        return jsonify({
            'message': f'Error during incremental harvest: {str(e)}',
            'last_run': last_run_time.isoformat()
        }), 500