# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

"""
Push to Elasticsearch
"""
from pathlib import Path
import json
import math

# Load posts and comments
posts_path = Path("data/posts.json")
comments_path = Path("data/comments.json")

with posts_path.open() as f:
    posts = json.load(f)

with comments_path.open() as f:
    comments = json.load(f)

# Example split size
max_docs_per_batch = 2000

# Splitting posts into batches
total_post_batches = math.ceil(len(posts) / max_docs_per_batch)

# Determine number of top-level comments
total_comments = sum(len(cdict) for cdict in comments.values())

# Output some statistics
print(f"Total post batches: {len(posts)}")
print(f"Total posts: {total_post_batches}")
print(f"Total comment threads:{len(comments)}")
print(f"Total individual comments:{total_comments}")