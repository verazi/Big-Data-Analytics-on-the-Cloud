# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

import zstandard
import os
import json
import ndjson
import sys
from datetime import datetime, tzinfo
import logging.handlers
import re
from typing import Optional, Dict, List, Tuple
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close()

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


def KeyWordsPresenceInSubmission(submission, keywords):
	"""Check if any of the keywords are present in the title or text of the submission."""

	if (submission["title"] and any(keyword in submission["title"] for keyword in keywords)):
		return True
	if (submission["selftext"] and any(keyword in submission["selftext_html"] for keyword in keywords)):
		return True
	return False

def KeyWordPresenceInSubmission(submission, keyword):
	"""Check if the keyword 'housing' is present in the title or text of the submission."""

	if (submission["title"] and keyword in submission["title"]):
		return True
	if (submission["selftext"] and keyword in submission["selftext_html"]):
		return True
	return False

def KeyWordsPresenceInComment(comment, keywords):
	"""Check if any of the keywords are present in the body of the comment."""

	if (comment["body"] and any(keyword in comment["body"] for keyword in keywords)):
		return True
	return False

def ConvertSubmissionToStandardFormat(submission):
	"""Convert the line of data to the standard format."""
	
	state = 'UNKNOWN'  # Default state, can be updated based on submission data
	text =  submission["title"] + " " + clean_content(submission["selftext_html"])
	# score = 0.0
	# sentiment = 'neutral'
	score, sentiment = analyze_sentiment(text)
	
	# prepare post action with unique _id
	standardDict = {
			'post_id': submission['id'],
			'platform': 'reddit',
			'author': submission['author'],
			'content': text,
			'created_at': datetime.fromtimestamp(int(submission['created_utc'])).isoformat(),
			'tags': submission['subreddit'],
			'state': state,
			'sentiment': sentiment,
			'sentiment_score': score
		}

	return standardDict

def ConvertCommentToStandardFormat(comment):
	"""Convert the line of data to the standard format."""
	
	state = 'UNKNOWN'  # Default state, can be updated based on comment data
	text = clean_content(comment["body"])
	# score = 0.0
	# sentiment = 'neutral'
	score, sentiment = analyze_sentiment(text)
	
	# prepare post action with unique _id
	standardDict = {
			'post_id': comment['id'],
			'post_id': comment['parent_id'],
			'platform': 'reddit',
			'author': comment['author'],
			'content': text,
			'created_at': datetime.fromtimestamp(int(comment['created_utc'])).isoformat(),
			'state': state,
			'sentiment': sentiment,
			'sentiment_score': score
	}

	return standardDict


if __name__ == "__main__":



	file_lines = 0
	file_bytes_processed = 0
	bad_lines = 0
	
	keywords = ['housing', 'affordability', 'rent', 'mortgage']

	SubmissionZstPath = r"E:\Unimelb Extras\reddit\subreddits24\AusFinance_submissions.zst"
	CommentsZstPath = r"E:\Unimelb Extras\reddit\subreddits24\AusFinance_comments.zst"

	outputDir = r"E:\Unimelb Extras\reddit\subreddits24\NDJSON"

	outputSubmissionList = []
	outputCommentList = []

	postCommand = {"index": {"_index": "housing-posts"}}
	commentCommand = {"index": {"_index": "housing-comments"}}

	for line, file_bytes_processed in read_lines_zst(SubmissionZstPath):
		try:
			obj = json.loads(line)
			if (KeyWordsPresenceInSubmission(obj, keywords)):
				standardised =  ConvertSubmissionToStandardFormat(obj)
				if (standardised is not None):
					outputSubmissionList.append(postCommand)
					outputSubmissionList.append(standardised)
		except (KeyError, json.JSONDecodeError) as err:
			bad_lines += 1
		file_lines += 1
		if file_lines % 100000 == 0:
			log.info(f"{file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}%")

	
	for line, file_bytes_processed in read_lines_zst(CommentsZstPath):
		try:
			obj = json.loads(line)
			if (KeyWordsPresenceInComment(obj, keywords)):
				standardised = ConvertCommentToStandardFormat(obj)
				if (standardised is not None):
					outputCommentList.append(commentCommand)
					outputCommentList.append(standardised)
		except (KeyError, json.JSONDecodeError) as err:
			bad_lines += 1
		file_lines += 1
		if file_lines % 100000 == 0:
			log.info(f"{file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}%")

	outputSubmissionList.append("")
	outputCommentList.append("")

	with open(os.path.join(outputDir, "reddit-AusFinance-submissions.ndjson"), "w") as final:
		ndjson.dump(outputSubmissionList, final)

	with open(os.path.join(outputDir, "reddit-Finance-comments.ndjson"), "w") as final:
		ndjson.dump(outputCommentList, final)

	# except Exception as err:
	# 	log.info(err)

	log.info(f"Complete : {file_lines:,} : {bad_lines:,}")