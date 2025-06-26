# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

from elasticsearch import Elasticsearch
import pandas as pd

# Connect to local Elasticsearch
es = Elasticsearch("http://localhost:9200")

# === Settings ===
index_name = "housing_comments"
query = {
    "query": {
        "match_all": {}
    },
    "size": 10000
}

# === Fetch data ===
res = es.search(index=index_name, body=query, scroll="2m")
scroll_id = res["_scroll_id"]
hits = res["hits"]["hits"]

while True:
    scroll_res = es.scroll(scroll_id=scroll_id, scroll="2m")
    scroll_hits = scroll_res["hits"]["hits"]
    if not scroll_hits:
        break
    hits.extend(scroll_hits)

records = []
for hit in hits:
    source = hit["_source"]
    records.append({
        "sentiment_score": source.get("sentiment_score", None),
        "sentiment": source.get("sentiment", None),
        "topic": source.get("topic", None),
        "topic_label": source.get("topic_label", None),
        "state": source.get("state", None)
    })

df = pd.DataFrame(records)

# === Basic Statistics ===
print("Total comments:", len(df))
print("\nSentiment distribution:")
print(df["sentiment"].value_counts())

print("\nSentiment score (summary):")
print(df["sentiment_score"].describe())

print("\nTop 10 topics:")
print(df["topic"].value_counts().head(10))

print("\nTop 10 topic labels:")
print(df["topic_label"].value_counts().head(10))

print("\n=== State-wise Statistics ===")

states = df["state"].dropna().unique()
print("Unique states detected:", states)

for state in sorted(states):
    state_df = df[df["state"] == state]
    print(f"\nState: {state}")
    print(f" Total comments: {len(state_df)}")
    print(" Sentiment distribution:")
    print(state_df["sentiment"].value_counts())
    print(" Average sentiment score:", round(state_df["sentiment_score"].mean(), 3))
    print(" Top topic labels:")
    print(state_df["topic_label"].value_counts().head(3))