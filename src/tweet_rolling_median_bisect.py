"""Program to calculate rolling median for tweet streams"""
import os
import sys
import bisect
import collections

class RollingMedian(object):
    """Compute rolling median using two list approach."""
    def __init__(self, window_size):
        self.window_size = window_size
        self.first_list = []
        self.second_list = collections.deque()

    def get_median(self, value):
        """Compute the median of the list using window method"""
        # Add new value to both lists
        self.second_list.append(value)
        bisect.insort(self.first_list, value)

        # verify if the rolling window size is reached
        # if yes, the oldest entry is removed from first list
        # its corresponding entry is removed from the second list
        if len(self.first_list) > self.window_size:
            old_value = self.second_list.popleft()
            del self.first_list[bisect.bisect_left(self.first_list, old_value)]

        # Get median based on even or odd length
        mid = len(self.first_list) / 2

        if len(self.first_list) % 2:
            current_median = self.first_list[mid]
        else:
            current_median = (self.first_list[mid] + self.first_list[mid-1]) / 2.0

        return current_median


def main():
    """
    Function to compute the rolling median for the tweets from input file.

    Input: Two Command line arguments, Input file path and Output file path
    Output: Rolling median of each tweet separated by new line
    """
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

    tweets = open(sys.argv[1]).readlines()
    out_file = open(sys.argv[2], "w")

    # Compute the unique word length for all the tweets
    unique_word_length = [len(set(tweet.split())) for tweet in tweets]
    rolling_median = RollingMedian(len(unique_word_length))

    for length in unique_word_length:
        out_file.write('{0:.2f}'.format(rolling_median.get_median(length))+"\n")

if __name__ == "__main__":
    main()
