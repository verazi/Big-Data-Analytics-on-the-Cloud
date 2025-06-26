# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

from elasticsearch import Elasticsearch, helpers
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from flask import jsonify
from typing import Tuple
import re
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Read configuration from ConfigMap
def config(key: str) -> str:
    with open(f'/configs/default/shared-data/{key}', 'r') as f:
        return f.read().strip()

# Initialize Elasticsearch client globally
es = Elasticsearch(
    hosts="https://elasticsearch-master.elastic.svc.cluster.local:9200",
    verify_certs=False,
    http_auth=(config("ES_USERNAME"), config("ES_PASSWORD"))
)

# Disable SSL warnings
urllib3.disable_warnings(category=InsecureRequestWarning)

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

# Main entrypoint for Fission
def main():
    post_src = "housing-posts"
    post_dst = "posts"
    comment_src = "housing-comments"
    comment_dst = "comments"

    actions = []
    BULK = 100

    # Posts
    for doc in helpers.scan(es, index=post_src, query={"query": {"match_all": {}}}):
        _id = doc["_id"]
        if es.exists(index=post_dst, id=_id):
            continue

        src = doc["_source"]
        cleaned = clean_content(src.get("content",""))
        score, label = analyze_sentiment(cleaned)

        actions.append({
            "_op_type": "index",
            "_index": post_dst,
            "_id": _id,
            "_source": {
                **src,
                "content": cleaned,
                "sentiment_score": score,
                "sentiment": label
            }
        })

        if len(actions) >= BULK:
            helpers.bulk(es, actions, chunk_size=BULK, raise_on_error=False)
            actions.clear()

    # Flush any remaining actions
    if actions:
        helpers.bulk(es, actions, chunk_size=BULK, raise_on_error=False)
        actions.clear()

    # Comments
    for doc in helpers.scan(es, index=comment_src, query={"query": {"match_all": {}}}):
        _id = doc["_id"]
        if es.exists(index=comment_dst, id=_id):
            continue

        src = doc["_source"]
        cleaned = clean_content(src.get("content",""))
        score, label = analyze_sentiment(cleaned)

        actions.append({
            "_op_type": "index",
            "_index": comment_dst,
            "_id": _id,
            "_source": {
                **src,
                "content": cleaned,
                "sentiment_score": score,
                "sentiment": label
            }
        })

        if len(actions) >= BULK:
            helpers.bulk(es, actions, chunk_size=BULK, raise_on_error=False)
            actions.clear()

    if actions:
        helpers.bulk(es, actions, chunk_size=BULK, raise_on_error=False)
        actions.clear()

    return jsonify({"message": "Reindex completed"}), 200