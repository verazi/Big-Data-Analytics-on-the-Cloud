"""
# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)
"""

"""
    Read files from harvesters
    Clean data and Analysis data (vader + BERTopic)
    Upload to ES
"""

import argparse
import pandas as pd
import requests
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer

def parse_args():
    p = argparse.ArgumentParser(description="Bulk insert posts & comments into Elasticsearch")
    p.add_argument("--posts",    required=True, help="Path to merged posts JSON-lines file")
    p.add_argument("--comments", required=True, help="Path to merged comments JSON-lines file")
    p.add_argument("--es-host",  default="http://localhost:9200", help="Elasticsearch host URL")
    return p.parse_args()

def clean_text(text):
    if pd.isna(text):
        return ""
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip().lower()

def main():
    args = parse_args()

    # === Step 1: Load data ===
    posts_df    = pd.read_json(args.posts,    lines=True)
    comments_df = pd.read_json(args.comments, lines=True)

    print("Posts columns:", posts_df.columns.tolist())
    print("Comments columns:", comments_df.columns.tolist())

    # === Step 2: Clean text ===
    posts_df["text"]    = posts_df["content"].apply(clean_text)
    comments_df["text"] = comments_df["content"].apply(clean_text)

    # Filter out low-quality comments
    comments_df = comments_df[
        (~comments_df["text"].isin(["", "removed", "deleted"])) &
        (comments_df["text"].str.split().str.len() >= 5)
    ]

    # to_datetime → nanoseconds int → milliseconds
    for df in (posts_df, comments_df):
        if "created_at" in df.columns:
            df["created_at"] = (pd.to_datetime(df["created_at"], utc=True).astype('int64') // 10 ** 6)

    # === Step 3: Sentiment Analysis ===
    analyzer = SentimentIntensityAnalyzer()
    comments_df["sentiment_score"] = comments_df["text"].apply(
        lambda t: analyzer.polarity_scores(t)["compound"]
    )
    comments_df["sentiment"] = comments_df["sentiment_score"].apply(
        lambda s: "positive" if s >= 0.05 else ("negative" if s <= -0.05 else "neutral")
    )

    # === Step 4: Topic Modeling on comments ===
    comments_df["topic"]       = 0
    comments_df["topic_label"] = "default"

    # # BERTopic only for Reddit
    # reddit_comments = comments_df[comments_df["platform"] == "reddit"]
    # if len(reddit_comments) >= 2:
    #     n_docs = len(reddit_comments)
    #     vec = CountVectorizer(stop_words="english", ngram_range=(1,2), min_df=1)
    #     model = BERTopic(vectorizer_model=vec)
    #     topics, _ = model.fit_transform(reddit_comments["text"].tolist())
    #     topic_info = model.get_topic_info().set_index("Topic")["Name"].to_dict()

    #     comments_df.loc[reddit_comments.index, "topic"] = topics
    #     comments_df.loc[reddit_comments.index, "topic_label"] = [topic_info.get(t, "default") for t in topics]


    # === Step 5: Upload to Elasticsearch ===
    host = args.es_host.rstrip("/")

    # Upload comments
    for _, row in comments_df.iterrows():
        doc = row.dropna().to_dict()
        url = f"{host}/housing_comments/_doc/{doc['comment_id']}"
        res = requests.put(url, json=doc)
        print(f"PUT {url} → {res.status_code} {res.text}") # debugging

    # post topic from comments
    # if "topic" in comments_df:
    #     comments_df["post_id"] = comments_df["post_id"].astype(str)
    #     posts_df["post_id"] = posts_df["post_id"].astype(str)
    #     top_topic = (
    #         comments_df.groupby("post_id")["topic"]
    #         .agg(lambda s: s.value_counts().idxmax())
    #         .rename("topic")
    #     )
    #     top_label = (
    #         comments_df.groupby("post_id")["topic_label"]
    #         .agg(lambda s: s.value_counts().idxmax())
    #         .rename("topic_label")
    #     )
    #     posts_df = posts_df.merge(top_topic, on="post_id", how="left")
    #     posts_df = posts_df.merge(top_label, on="post_id", how="left")

    # posts_df["topic"]       = posts_df.get("topic", 0).fillna(0).astype(int)
    # posts_df["topic_label"] = posts_df.get("topic_label", "default").fillna("default")

    # Upload posts
    for _, row in posts_df.iterrows():
        doc = row.dropna().to_dict()
        url = f"{host}/housing_posts/_doc/{doc['post_id']}"
        res = requests.put(url, json=doc)
        print(f"PUT {url} → {res.status_code} {res.text}") # debugging

    print("Insert completed")

if __name__ == "__main__":
    main()
