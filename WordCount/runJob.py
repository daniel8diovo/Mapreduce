from WordCount import MRWordCount
import sys
import time

word_frequency_pairs = []
t_start = time.process_time()
job_args = ['-r', 'local']
job_args.extend(sys.argv[1:])
mr_job = MRWordCount(args=job_args)
with mr_job.make_runner() as runner:
	runner.run()
	for word, frequency in mr_job.parse_output(runner.cat_output()):
		word_frequency_pairs.append([word, frequency])
	
	t_end = time.process_time() - t_start
	
	for distinct_word in word_frequency_pairs:
		print(distinct_word[0], " ", distinct_word[1])
	
	print("Execution Time: ", t_end)