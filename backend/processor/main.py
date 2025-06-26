"""
# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)
"""

"""
    main
    1. trigger harvesters (each max 2 mins for testing)
    2. wait for data/**/*.json
    3. merge data/**/*.json → database/posts.json & database/comments.json
    4. trigger upload_to_es.py
"""

import subprocess
import glob
import os
import sys
import time
import json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR   = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))


HARVEST_TIMEOUT = 60 * 2 # for debug
HARVESTERS = [
    {
        "script":     os.path.join(BASE_DIR, "backend", "harvesters", "reddit_harvester.py"),
        "data_glob":  os.path.join(BASE_DIR, "data", "reddit", "**", "posts.json")
    },
    {
        "script":     os.path.join(BASE_DIR, "backend", "harvesters", "mastodon_harvester.py"),
        "data_glob":  os.path.join(BASE_DIR, "data", "mastodon", "**", "comments.json")
    }
]

POSTS_JSON    = os.path.join(BASE_DIR, "database", "posts.json")
COMMENTS_JSON = os.path.join(BASE_DIR, "database", "comments.json")
UPLOAD_SCRIPT = os.path.join(BASE_DIR, "backend", "processor", "upload_to_es.py")

def run_harvester(path, timeout=HARVEST_TIMEOUT):
    print(f"▶ Running harvester (timeout {timeout}s): {path}")
    try:
        subprocess.run([sys.executable, path], check=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"[Warn] Harvester timed out: {path}")
    except subprocess.CalledProcessError as e:
        print(f"[Error] Harvester failed: {path} → {e}")

def wait_for_any(pattern, timeout=300, interval=2):
    print(f"Waiting for files matching: {pattern}")
    start = time.time()
    while time.time() - start < timeout:
        matches = glob.glob(pattern, recursive=True)
        if matches:
            print(f"Found {len(matches)} files")
            return
        time.sleep(interval)
    print(f"[Warn] No files found for pattern within timeout: {pattern}")

if __name__ == "__main__":
    for fn in (POSTS_JSON, COMMENTS_JSON):
        try:
            os.remove(fn)
        except FileNotFoundError:
            pass

    # 1. trigger each harvester
    for h in HARVESTERS:
        run_harvester(h["script"])
        wait_for_any(h["data_glob"])

    # 2. merge data/**/*.json → database/*.json
    os.makedirs(os.path.dirname(POSTS_JSON), exist_ok=True)
    os.makedirs(os.path.dirname(COMMENTS_JSON), exist_ok=True)

    print(f"▶ Merging all posts to {POSTS_JSON}")
    with open(POSTS_JSON, "w", encoding="utf-8") as p_out:
        for fn in glob.glob(os.path.join(BASE_DIR, "data", "**", "posts.json"), recursive=True):
            state = os.path.basename(os.path.dirname(fn))
            platform = os.path.basename(os.path.dirname(os.path.dirname(fn)))
            with open(fn, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    obj["state"] = state
                    obj["platform"] = platform
                    p_out.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"▶ Merging all comments to {COMMENTS_JSON}")
    with open(COMMENTS_JSON, "w", encoding="utf-8") as c_out:
        for fn in glob.glob(os.path.join(BASE_DIR, "data", "**", "comments.json"), recursive=True):
            state = os.path.basename(os.path.dirname(fn))
            platform = os.path.basename(os.path.dirname(os.path.dirname(fn)))
            with open(fn, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    obj["state"] = state
                    obj["platform"] = platform
                    c_out.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # 3. call upload_to_es.py
    cmd = [
        sys.executable,
        UPLOAD_SCRIPT,
        "--posts",    POSTS_JSON,
        "--comments", COMMENTS_JSON
    ]
    print("▶ Running upload_to_es:", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[Error] upload_to_es failed: {e}")
    else:
        print("Completed!")