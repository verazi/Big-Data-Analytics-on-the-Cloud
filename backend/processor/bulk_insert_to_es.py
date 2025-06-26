"""
# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)
"""

"""
    Hardcode json files to ES
"""

import pandas as pd
import requests
import re
from urllib.parse import urlparse
from sklearn.feature_extraction.text import CountVectorizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from bertopic import BERTopic


# === Step 1: Load data ===
posts_df = pd.read_json("database/comments.json")
comments_df = pd.read_json("database/posts.json").transpose()

# === Step 2: Flatten nested comment structure ===
def flatten_comments(df):
    flattened = []
    for post_id, comments in df.items():
        for comment_id, comment in comments.items():
            if isinstance(comment, dict):
                comment["comment_id"] = f"{post_id}_{comment_id}"
                comment["post_id"] = post_id
                flattened.append(comment)
    return flattened

comments_flat = flatten_comments(comments_df)
comments_df_flat = pd.DataFrame(comments_flat)

# === Step 3: Merge comments with selected post fields ===
merged_df = comments_df_flat.merge(
    posts_df[["id", "title", "url"]],
    left_on="post_id",
    right_on="id",
    how="left"
)

# === Step 4: Infer state from post title ===
def infer_state(title, url):
    title = str(title).lower()
    if "melbourne" in title or "melb" in title or "victoria" in title or "vic" in title:
        return "VIC"
    elif "sydney" in title or "syd" in title or "nsw" in title:
        return "NSW"
    elif "queensland" in title or "brisbane" in title or "qld" in title:
        return "QLD"
    elif "perth" in title or "wa" in title:
        return "WA"
    elif "adelaide" in title or "sa" in title:
        return "SA"
    elif "tasmania" in title or "hobart" in title or "tas" in title:
        return "TAS"
    elif "darwin" in title or "nt" in title:
        return "NT"

    from urllib.parse import urlparse
    path = urlparse(str(url)).path.lower()
    if "/r/melbourne" in path: return "VIC"
    if "/r/sydney" in path: return "NSW"
    if "/r/brisbane" in path: return "QLD"
    if "/r/perth" in path: return "WA"
    if "/r/adelaide" in path: return "SA"
    if "/r/hobart" in path: return "TAS"
    if "/r/darwin" in path: return "NT"

    return ""

posts_df["state"] = posts_df.apply(lambda row: infer_state(row["title"], row["url"]), axis=1)
merged_df["state"] = merged_df.apply(lambda row: infer_state(row["title"], row["url"]), axis=1)

# Clean the raw data
def clean_text(text):
    if pd.isna(text): return ""
    text = re.sub(r"http\S+", "", text)               # remove URL
    text = re.sub(r"@\w+", "", text)                  # remove @username
    text = re.sub(r"[^\w\s]", "", text)               # remove punctuations and special words
    return text.strip().lower()

# Apply cleaning and rename
renamed_df = merged_df.rename(columns={
    "body": "text",
    "author": "author",
    "score": "score"
})
renamed_df["text"] = renamed_df["text"].apply(clean_text)
# Filter out empty, removed, or too short
renamed_df = renamed_df[
    (~renamed_df["text"].isin(["removed", "deleted"])) &
    (renamed_df["text"].str.split().str.len() >= 5)
]
post_state_map = posts_df.set_index("id")["state"].to_dict()
renamed_df["state"] = renamed_df["post_id"].map(post_state_map)
print(renamed_df[["post_id", "state"]].drop_duplicates().head(10))

# === Sentiment Analysis with VADER ===
analyzer = SentimentIntensityAnalyzer()
def get_sentiment(text):
    if pd.isna(text): return "neutral"
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"

def get_sentiment_score(text):
    if pd.isna(text): return 0.0
    return analyzer.polarity_scores(text)["compound"]

renamed_df["sentiment"] = renamed_df["text"].apply(get_sentiment)
renamed_df["sentiment_score"] = renamed_df["text"].apply(get_sentiment_score)

# === Topic Modeling with BERTopic ===
vectorizer_model = CountVectorizer(
    stop_words="english",
    ngram_range=(1, 2),
    min_df=5
)
topic_model = BERTopic(vectorizer_model=vectorizer_model)
topics, _ = topic_model.fit_transform(renamed_df["text"].tolist())
renamed_df["topic"] = topics

topic_info = topic_model.get_topic_info()
topic_labels = topic_info.set_index("Topic")["Name"].to_dict()
renamed_df["topic_label"] = renamed_df["topic"].map(topic_labels)

# === Step 5a: Prepare comments upload DataFrame ===
comment_fields = [
    "comment_id", "post_id", "text", "author", "score", "title", "url", "state",
    "sentiment", "sentiment_score", "topic", "topic_label"
]
comments_to_upload = renamed_df[comment_fields]

# === Step 5b: Prepare posts upload DataFrame ===
# Assign post.topic as most common comment.topic
top_topic_by_post = renamed_df.groupby("post_id")["topic"].agg(lambda x: x.value_counts().index[0]).reset_index()
top_label_by_post = renamed_df.groupby("post_id")["topic_label"].agg(lambda x: x.value_counts().index[0]).reset_index()
posts_to_upload = posts_df.rename(columns={"id": "post_id"}).drop_duplicates("post_id")
posts_to_upload = posts_to_upload.merge(top_topic_by_post, on="post_id", how="left")
posts_to_upload = posts_to_upload.merge(top_label_by_post, on="post_id", how="left")

# posts_to_upload = posts_df.rename(columns={"id": "post_id"}).drop_duplicates("post_id")

# === Step 6a: Upload comments to ES ===
print(f"Uploading comments: {len(comments_to_upload)} records")
comment_result = {"success": 0, "fail": 0}

for _, row in comments_to_upload.iterrows():
    try:
        doc = row.dropna().to_dict()
        url = f"http://localhost:9200/housing_comments/_doc/{doc['comment_id']}"
        res = requests.put(url, json=doc)
        if res.status_code in [200, 201]:
            comment_result["success"] += 1
        else:
            print("Comment Failed:", res.status_code, res.text)
            comment_result["fail"] += 1
    except Exception as e:
        print("Comment Error:", str(e))
        comment_result["fail"] += 1

# === Step 6b: Upload posts to ES ===
print(f"Uploading posts: {len(posts_to_upload)} records")
post_result = {"success": 0, "fail": 0}

for _, row in posts_to_upload.iterrows():
    try:
        doc = row.dropna().to_dict()
        url = f"http://localhost:9200/housing_posts/_doc/{doc['post_id']}"
        res = requests.put(url, json=doc)
        if res.status_code in [200, 201]:
            post_result["success"] += 1
        else:
            print("Post Failed:", res.status_code, res.text)
            post_result["fail"] += 1
    except Exception as e:
        print("Post Error:", str(e))
        post_result["fail"] += 1

# === Summary ===
print("Upload completed:")
print(" Comments:", comment_result)
print(" Posts:", post_result)