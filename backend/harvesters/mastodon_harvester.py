# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

import os
import time
import json
import getpass
from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser
import requests

# Prompt for credentials
API_BASE_URL = 'https://aus.social'
ACCESS_TOKEN = getpass.getpass("Mastodon ACCESS_TOKEN: ").strip()
STATES       = ['ACT','NSW','NT','QLD','SA','TAS','VIC','WA']
HASHTAGS     = ['housing','affordability','rent','mortgage']
KEYWORDS     = HASHTAGS
START_DATE   = datetime.now(timezone.utc) - timedelta(days=365*10)

HEADERS = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Accept':        'application/json'
}

def safe_get(url, params=None, retries=5):
    for _ in range(retries):
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code == 429:
            requestTimestamp = resp.headers.get('Date')
            requestTimestamp = datetime.strptime(requestTimestamp, '%a, %d %b %Y %H:%M:%S %Z')
            resetTimestamp = resp.headers.get('X-RateLimit-Reset')
            resetTimestamp = datetime.strptime(resetTimestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
            wait = max((resetTimestamp - requestTimestamp).total_seconds(), 60)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"Failed GET {url} after {retries} retries")


def fetch_tag_page(tag, max_id=None):
    url = f"{API_BASE_URL}/api/v1/timelines/tag/{tag}"
    params = {'limit':40}
    if max_id:
        params['max_id'] = max_id
    return safe_get(url, params=params).json()


def fetch_replies(post_id):
    url = f"{API_BASE_URL}/api/v1/statuses/{post_id}/context"
    data = safe_get(url).json().get('descendants', [])
    comments = []
    for r in data:
        created = date_parser.parse(r['created_at'])
        comments.append({
            'comment_id': r['id'],
            'post_id':    post_id,
            'platform':   'mastodon',
            'author':     r['account']['acct'],
            'content':    r['content'],
            'created_at': created.isoformat()
        })
    return comments

if __name__ == '__main__':
    for state in STATES:
        base = os.path.join('data','mastodon',state)
        os.makedirs(base, exist_ok=True)
        posts_fp = os.path.join(base,'posts.json')
        comments_fp = os.path.join(base,'comments.json')
        print(f"\n--- Mastodon STATE={state} ---")

        # Load already-harvested post and comment IDs to skip duplicates
        seen_posts = set()
        if os.path.exists(posts_fp):
            with open(posts_fp,'r',encoding='utf-8') as f:
                for line in f:
                    try:
                        seen_posts.add(json.loads(line)['post_id'])
                    except Exception:
                        continue
        seen_comments = set()
        if os.path.exists(comments_fp):
            with open(comments_fp,'r',encoding='utf-8') as f:
                for line in f:
                    try:
                        seen_comments.add(json.loads(line)['comment_id'])
                    except Exception:
                        continue

        max_id = None
        with open(posts_fp,'a',encoding='utf-8') as p_out, open(comments_fp,'a',encoding='utf-8') as c_out:
            for tag in HASHTAGS:
                while True:
                    statuses = fetch_tag_page(tag, max_id)
                    if not statuses:
                        break
                    for st in statuses:
                        sid = st['id']
                        if sid in seen_posts:
                            continue
                        created = date_parser.parse(st['created_at'])
                        if created < START_DATE:
                            max_id = None
                            break
                        text = st.get('content','') or ''
                        if any(kw in text.lower() for kw in KEYWORDS):
                            post_doc = {
                                'post_id':    sid,
                                'platform':   'mastodon',
                                'author':     st['account']['acct'],
                                'content':    text,
                                'created_at': created.isoformat(),
                                'tags':       [tag]
                            }
                            p_out.write(json.dumps(post_doc, ensure_ascii=False) + "\n")
                            seen_posts.add(sid)
                            replies = fetch_replies(sid)
                            for c in replies:
                                cid = c['comment_id']
                                if cid in seen_comments:
                                    continue
                                c_out.write(json.dumps(c, ensure_ascii=False) + "\n")
                                seen_comments.add(cid)
                            print(f"Saved {sid} (+{len(replies)} replies)")
                    if not statuses or (created < START_DATE):
                        break
                    max_id = int(statuses[-1]['id']) - 1
                    time.sleep(1)