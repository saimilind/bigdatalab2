from mrjob.job import MRJob
import re

class MRNonStopWordCounter(MRJob):

    def mapper(self, _, line):
        # Split the line into words
        words = re.findall(r'\w+', line.lower())

        # List of stopwords
        stopwords = {'the', 'and', 'of', 'a', 'to', 'in', 'is', 'it'}

        # Emit word count for each non-stop word
        for word in words:
            if word not in stopwords:
                yield (word, 1)

    def reducer(self, word, counts):
        # Sum the counts for each word
        yield (word, sum(counts))

if __name__ == '__main__':
    MRNonStopWordCounter.run()
