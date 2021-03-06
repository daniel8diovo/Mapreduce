#!/usr/bin/env
from MRInvertedIndex import MRInvertedIndex
import sys
import time

word_line_numbers_pairs = []
t_start = time.process_time()
job_args = ['-r', 'local']
job_args.extend(sys.argv[1:])
mr_job = MRInvertedIndex(args=job_args)
with mr_job.make_runner() as runner:
	runner.run()
	for word, line_numbers in mr_job.parse_output(runner.cat_output()):
		word_line_numbers_pairs.append([word, line_numbers])
	
	t_end = time.process_time() - t_start
	
	for distinct_word in word_line_numbers_pairs[:50]:
		print(distinct_word[0], " ", distinct_word[1])#Limit output to 50 as stated in lab brief.
	
	print("Execution Time: ", t_end)