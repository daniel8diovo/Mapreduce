import mrs
import string
import nltk
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

STOPWORDS = nltk.corpus.stopwords.words('english')
WORD_RE = re.compile(r"[\w']+")


class MRWordCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words)
        ]

    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            word = word.strip(string.punctuation).lower()
            if len(word) > 2 and word.lower() not in STOPWORDS and not word.isdigit():
                yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield (word, sum(counts))


if __name__ == '__main__':
    MRWordCount.run()