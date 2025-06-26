# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

import json
import ndjson
import os
 
filePath =  r"E:\Unimelb Extras\reddit\subreddits24\JSON\reddit-melbourne-submissions.json"

outputDir = r"E:\Unimelb Extras\reddit\subreddits24\NDJSON"

with open(filePath, 'r') as file:
	data = json.load(file)

	with open(os.path.join(outputDir, "reddit-sydney-comments.ndjson"), "w") as f:
		ndjson.dump(data, f)
