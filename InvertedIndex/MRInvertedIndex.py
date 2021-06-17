#!/usr/bin/env 
from mrjob.job  import MRJob
from mrjob.step import MRStep
import nltk
import string
import re
	
WORD_RE     = re.compile(r"[\w']+") #looks for words such as "word", word's "word's", word
STOPWORDS = nltk.corpus.stopwords.words('english')

class MRInvertedIndex(MRJob):
	def steps(self):
	
		return [MRStep(mapper   = self.mapper_get_word_locations,
						 reducer  = self.reducer_words_locations_list)]	
						 	 
	def mapper_get_word_locations(self, _, line):
		key, line = line.split('\t', 1)
		# yield each word in the line and the line number in key
		for word in WORD_RE.findall(line):
			word = word.strip(string.punctuation).lower()
			if word and word.lower() not in STOPWORDS and not word.isdigit():
				yield (word.lower(), key)
	
	def reducer_words_locations_list(self, word, line_numbers):
		line_numbers_list = list(line_numbers)#create list object
		yield (word, line_numbers_list)
		
if __name__ == '__main__':

	MRInvertedIndex.run()
