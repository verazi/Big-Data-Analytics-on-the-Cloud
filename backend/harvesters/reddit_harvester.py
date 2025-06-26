# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

import os
import json
import getpass
import time
from datetime import datetime, timedelta, timezone
import praw
import prawcore

# Prompt for credentials
CLIENT_ID     = 'Sm5Hyq-3pIgg_Gqb9RLmOQ'
CLIENT_SECRET = getpass.getpass("Reddit CLIENT_SECRET: ").strip()
USER_AGENT    = 'HousingSentimentBot/0.1 by u/Emotional-Exam-3670'
STATES        = ['ACT','NSW','NT','QLD','SA','TAS','VIC','WA']
SUBREDDITS    = ['Australia','Melbourne','housing','AusFinance']
KEYWORDS      = ['housing','house prices','affordability','rent','mortgage']
MAX_KW        = 500
START_TS      = (datetime.now(timezone.utc) - timedelta(days=365*10)).timestamp()

reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)

def reddit_safe_call(func, *args, retries=5, **kwargs):
    for _ in range(retries):
        try:
            return func(*args, **kwargs)
        except prawcore.TooManyRequests as e:
            wait = int(e.response.headers.get('Retry-After', 60))
            print(f"[Reddit 429] sleeping {wait}s…")
            time.sleep(wait)
    raise RuntimeError(f"Reddit call failed: {func.__name__}")

def fetch_comments(subm, seen_comments):
    subm.comments.replace_more(limit=None)
    out = []
    for c in subm.comments.list():
        cid = c.id
        if cid in seen_comments:
            continue
        comment = {
            'comment_id': cid,
            'post_id':    subm.id,
            'platform':   'reddit',
            'author':     str(c.author),
            'content':    c.body,
            'created_at': datetime.fromtimestamp(c.created_utc, timezone.utc).isoformat()
        }
        out.append(comment)
        seen_comments.add(cid)
    return out

if __name__ == '__main__':
    for state in STATES:
        base = os.path.join('data','reddit',state)
        os.makedirs(base, exist_ok=True)
        posts_fp = os.path.join(base,'posts.json')
        comments_fp = os.path.join(base,'comments.json')
        print(f"\n--- Reddit STATE={state} ---")

        # Load existing IDs
        seen_posts = set()
        if os.path.exists(posts_fp):
            with open(posts_fp,'r',encoding='utf-8') as f:
                for line in f:
                    try:
                        seen_posts.add(json.loads(line)['post_id'])
                    except:
                        continue
        seen_comments = set()
        if os.path.exists(comments_fp):
            with open(comments_fp,'r',encoding='utf-8') as f:
                for line in f:
                    try:
                        seen_comments.add(json.loads(line)['comment_id'])
                    except:
                        continue

        with open(posts_fp,'a',encoding='utf-8') as p_out, open(comments_fp,'a',encoding='utf-8') as c_out:
            for sub in SUBREDDITS:
                for kw in KEYWORDS:
                    submissions = reddit_safe_call(reddit.subreddit(sub).search, kw, time_filter='all', limit=MAX_KW)
                    for subm in submissions:
                        sid = subm.id
                        if sid in seen_posts or subm.created_utc < START_TS:
                            continue
                        text = subm.title + ("\n\n"+subm.selftext if subm.selftext else "")
                        post_doc = {
                            'post_id':    sid,
                            'platform':   'reddit',
                            'author':     str(subm.author),
                            'content':    text,
                            'created_at': datetime.fromtimestamp(subm.created_utc, timezone.utc).isoformat(),
                            'tags':       [sub]
                        }
                        p_out.write(json.dumps(post_doc, ensure_ascii=False) + "\n")
                        seen_posts.add(sid)
                        comments = reddit_safe_call(fetch_comments, subm, seen_comments)
                        for comment in comments:
                            c_out.write(json.dumps(comment, ensure_ascii=False) + "\n")
                        print(f"Saved {sid} (+{len(comments)} comments)")
