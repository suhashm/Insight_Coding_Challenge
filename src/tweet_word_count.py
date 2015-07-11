"""Tweet word count calculation"""
import sys
import os
from collections import defaultdict


def main():
    """
    Function to compute the cardinality of words from the input file.

    Input: Two Command line arguments, Input file path and Output file path
    Output: Cardinality of word, sorted according to ASCII code
    """
    if len(sys.argv) < 3:
        print "Correct Usage 'python <input-file-path> <output-file-path>'"
        sys.exit(0)

    words = defaultdict(int)
    try:
        with open(sys.argv[1]) as input_file:
            for line in input_file:
                for word in line.split():
                    words[word] += 1
    except IOError:
        print "Input file not found. Please enter the correct path of the input file"

    # If output directory do not exist, create it
    dir_path = sys.argv[2].rfind("/")
    output_directory = sys.argv[2][:dir_path]

    try:
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
    except IOError:
        print "Unable to create an output directory, Kindly provide another output file path"

    # Write the word count in sorted order to the output file
    with open(sys.argv[2], "w") as out_file:
        for word in sorted(words):
            out_file.write(word+" "+str(words[word])+"\n")

if __name__ == "__main__":
    main()
