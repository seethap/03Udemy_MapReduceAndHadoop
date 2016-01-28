# to count words only (without paranthesis, period,
#can count word' and 'word.' using REGEX
from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w']+") #only consider alphabets and numbers
#WORD_REGEXP = re.compile(r"[A-Za-z]+") #only consider alphabets

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
    
# !python Word-Frequency-Better.py book.txt > wordsbetter.txt