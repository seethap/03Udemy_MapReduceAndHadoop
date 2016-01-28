# sort the frequency of words so as to know which word is used most
# using chain of mappers and reducers
from mrjob.job import MRJob
# Sorting by count
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_make_counts_key,
                   reducer = self.reducer_output_words)
        ]
        
    def mapper_get_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self, word, values):
        yield word, sum(values)
        
    def mapper_make_counts_key(self, word, count):
        yield '%04d'%int(count), word #MR does string sorting on keys
        
    def reducer_output_words(self, count, words):
        for word in words: #some words can have same count and will be grouped my mapper, so we extract them here 
            yield count, word


if __name__ == '__main__':
    MRWordFrequencyCount.run()
    
# !python Word-Frequency-Sorted.py book.txt > wordssorted.txt