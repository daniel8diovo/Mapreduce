
from MRKquery import MRTopKWords
import sys
import time

def getTopKWords(word_count_pairs_list, k):
	print("--------------------------")
	print("Top {} Most Frequent Words".format(k))
	print("--------------------------")
	
	if len(word_count_pairs_list) < k:
		k = len(word_count_pairs_list)
	list1 = []
	topKWordsList = []
	for value in word_count_pairs_list:
		list1.append(value)
	for i in range(k):
		topKWordsList.append(max(list1))
		list1.remove(max(list1))
	for i in range(k):
		print(f"{topKWordsList[i][1]:<20} {topKWordsList[i][0]}")
		
		
word_count_pairs = []
t_start = time.process_time()
job_args = ['-r', 'local']
job_args.extend(sys.argv[1:])
mr_job = MRTopKWords(args=job_args)
with mr_job.make_runner() as runner:
	runner.run()
	for key, word_count_pair in mr_job.parse_output(runner.cat_output()):
		word_count_pairs.append(word_count_pair)
	
	t_end = time.process_time() - t_start
	
	getTopKWords(word_count_pairs, 10)
	getTopKWords(word_count_pairs, 20)
	
	print("\nExecution Time: ", t_end)