"""Program to calculate the rolling median of streaming tweets"""
import os
import sys
import imp

try:
    imp.find_module('numpy')
    import numpy as np
except ImportError:
    print "Unable to find numpy module"
    print "Kindly install it by using the below command"
    print "sudo apt-get install numpy"
    sys.exit(0)


def main():
    """
    Function to compute the rolling median for the tweets from input file.

    Input: Two Command line arguments, Input file path and Output file path
    Output: Rolling median of each tweet separated by new line
    """
    current_median = 0

    if len(sys.argv) < 3:
        print "Incorrect Usage, Kindly follow"
        print "python <file_name> <input_file_path> <output_file_path>"
        sys.exit(0)

    # If output directory do not exist, create it
    dir_path = sys.argv[2].rfind("/")
    output_directory = sys.argv[2][:dir_path]

    try:
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
    except IOError:
        print "Unable to create output directory, Kindly provide another path"

    # For each tweet word count, compute median using numpy function
    median_array = np.array([])
    try:
        with open(sys.argv[1]) as input_file:
            with open(sys.argv[2], "w") as out_file:
                for line in input_file:
                    median_array = np.insert(median_array, 0, len(list(set(line.split()))))
                    current_median = np.median(median_array)
                    out_file.write('{0:.2f}'.format(current_median)+"\n")
    except IOError as error:
        print "Unable to Read/Write to disk ", error

if __name__ == "__main__":
    main()
