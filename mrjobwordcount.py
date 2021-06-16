# from mrjob.job import MRJob
# from mrjob.step import MRStep
# import re

# # Word frequency from book sorted by frequency
# # File: book.txt  

# # regular expression used to identify word
# WORD_REGEXP = re.compile(r"[\w']+")

# class MRWordFrequencyCount(MRJob):

#     def steps(self):
#         # 2 steps
#         return [
#             MRStep(mapper=self.mapper_get_words,
#                    reducer=self.reducer_count_words),
#             MRStep(mapper=self.mapper_make_counts_key,
#                    reducer=self.reducer_output_words)
#         ]

#     # Step 1
#     def mapper_get_words(self, _, line):
#         words = WORD_REGEXP.findall(line)
#         for w in words:
#             yield w.lower(), 1

#     def reducer_count_words(self, word, values):
#         yield word, sum(values)

#     # Step 2
#     def mapper_make_counts_key(self, word, count):
#         # sort by values
#         yield '%04d' % int(count), word

#     def reducer_output_words(self, count, words):
#         # First Column is the count
#         # Second Column is the word
#         for word in words:
#             yield count, word


# if __name__ == '__main__':
#     MRWordFrequencyCount.run()

import mrs
import string
import nltk
nltk.download('punkt')
from nltk.corpus import stopwords
nltk.download('stopwords')
from nltk.tokenize import word_tokenize
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

STOPWORDS = nltk.corpus.stopwords.words('english')
WORD_RE = re.compile(r"[\w']+")


class MRTopKWords(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            # MRStep(reducer=self.reducer_find_max_word)
            MRStep(reducer=self.topk)
        ]

    def mapper_get_words(self, _, line):
        # yield each word in the line
        self.lines = []
        for word in WORD_RE.findall(line):
            self.lines.append(line)
            text_tokens = word_tokenize(word)
            tokens_without_sw = [w for w in text_tokens if not w in stopwords.words()]
            result = ''.join([i for i in tokens_without_sw if not i.isdigit()])
            if result:
                if result.lower() in STOPWORDS:
                    continue
                yield (result.lower(), 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # # discard the key; it is just None
    # def reducer_find_max_word(self, _, word_count_pairs):
    #     # each item of word_count_pairs is (count, word),
    #     # so yielding one results in key=counts, value=word
    #     yield max(word_count_pairs)
    
    def topk(self, key, values):
        self.list1 = []
        self.list2 = []
        for value in values:
            self.list1.append(value)
        for i in range(10):
            self.list2.append(max(self.list1))
            self.list1.remove(max(self.list1))
        for i in range(10):
            yield self.list2[i]


if __name__ == '__main__':
    MRTopKWords.run()