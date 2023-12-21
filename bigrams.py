from mrjob.job import MRJob
import re

class MRBigramCounter(MRJob):

    def mapper(self, _, line):
        # Split the line into words
        words = re.findall(r'\w+', line.lower())

        # Emit bigrams
        for i in range(len(words)-1):
            bigram = f"{words[i]},{words[i+1]}"
            yield (bigram, 1)

    def reducer(self, bigram, counts):
        # Sum the counts for each bigram
        yield (bigram, sum(counts))

if __name__ == '__main__':
    MRBigramCounter.run()
