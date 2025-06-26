# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)

import os

def split_ndjson(input_file, lines_per_file, output_dir):
	if not os.path.exists(output_dir):
		os.makedirs(output_dir)
	with open(input_file, 'r', encoding='utf-8') as infile:
		file_count = 0
		lines = []
		for line_num, line in enumerate(infile, 1):
			lines.append(line)
			if line_num % lines_per_file == 0:
				out_path = os.path.join(output_dir, f'reddit-AusFinance-comments-part_{file_count}.ndjson')
				with open(out_path, 'w', encoding='utf-8') as outfile:
					outfile.writelines(lines)
				lines = []
				file_count += 1
		if lines:
			out_path = os.path.join(output_dir, f'reddit-AusFinance-comments-part_{file_count}.ndjson')
			with open(out_path, 'w', encoding='utf-8') as outfile:
				outfile.writelines(lines)


split_ndjson(r'E:\Unimelb Extras\reddit\subreddits24\NDJSON\reddit-Finance-comments.ndjson', 100000, r'E:\Unimelb Extras\reddit\subreddits24\NDJSON')