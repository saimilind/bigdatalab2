from mrjob.job import MRJob
import re

class MRInvertedIndex(MRJob):

    def mapper(self, _, line):
        # Use regular expression to find document name
        match = re.match(r'^(.*?):', line)
        if match:
            document = match.group(1)
        else:
            document = None

        # Split the line into words
        words = re.findall(r'\w+', line.lower())

        # Emit (word, document) pairs
        for word in words:
            if document:
                yield (word, document)

    def reducer(self, word, documents):
        # Collect all unique documents for each word
        yield (word, list(set(documents)))

if __name__ == '__main__':
    MRInvertedIndex.run()
