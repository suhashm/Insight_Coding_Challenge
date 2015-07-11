"""Program to calculate rolling median using heaps"""
import sys
import os
import heapq

MAX_HEAP = []
MIN_HEAP = []
heapq.heapify(MIN_HEAP)
heapq.heapify(MAX_HEAP)

def get_size():
    """get size of max heap and min heap"""
    if len(MIN_HEAP) == len(MAX_HEAP):
        return 0
    elif len(MIN_HEAP) < len(MAX_HEAP):
        return 1
    else:
        return -1

def get_median(val, current_median):
    """
    Function to compute the rolling median for the tweets from input file.

    Input:
        val: number of unique words per tweet
        current_median: effective median after processing each tweet
    Output: Rolling median of each tweet separated by new line
    """
    size_diff = get_size()

    # Both min and max heap consists of same number of elements
    if size_diff == 0:
        # if current word count is less than effective median
        # both heaps consists of same number of elements
        # Add new value to maxheap if value is less than current median
        # else add the value to minheap
        # median is the top most elements of MIN_HEAP or MAX_HEAP

        if val < current_median:
            heapq.heappush(MAX_HEAP, val * -1)
            current_median = heapq.nsmallest(1, MAX_HEAP)[0] * -1
            return current_median
        else:
            heapq.heappush(MIN_HEAP, val)
            current_median = heapq.nsmallest(1, MIN_HEAP)[0]
            return current_median

    # MAX_HEAP has more elements than MIN_HEAP
    elif size_diff == 1:
        if val < current_median:
            min_insert = heapq.heappop(MAX_HEAP)
            heapq.heappush(MIN_HEAP, min_insert * -1)
            heapq.heappush(MAX_HEAP, val * -1)
        else:
            heapq.heappush(MIN_HEAP, val)

        # Balanced heaps
        # Median is the average of root elements of MIN_HEAP and MAX_HEAP

        min_heap_root = heapq.nsmallest(1, MIN_HEAP)[0]
        max_heap_root = heapq.nsmallest(1, MAX_HEAP)[0] * -1
        current_median = (min_heap_root + max_heap_root) / (2 * 1.0)
        return current_median

    # MIN_HEAP has more elements than MAX_HEAP
    else:
        if val < current_median:
            heapq.heappush(MAX_HEAP, val * -1)
        else:
            max_insert = heapq.heappop(MIN_HEAP)
            heapq.heappush(MAX_HEAP, max_insert * -1)
            heapq.heappush(MIN_HEAP, val)

        # Balanced heaps
        # Median is the average of root elements of MIN_HEAP and MAX_HEAP
        min_heap_root = heapq.nsmallest(1, MIN_HEAP)[0]
        max_heap_root = heapq.nsmallest(1, MAX_HEAP)[0] * -1
        current_median = (min_heap_root + max_heap_root) / (2 * 1.0)
        return current_median


def main():
    """Main file to read the tweets and calculate rolling median"""
    current_median = 0
    if len(sys.argv) < 3:
        print "Correct Usage 'python <file-name> <input-file-path> <output-file-path>'"
        sys.exit(0)

    # If output directory do not exist, create it
    dir_path = sys.argv[2].rfind("/")
    output_directory = sys.argv[2][:dir_path]

    try:
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
    except IOError:
        print "Unable to create output directory, Kindly provide another path"

    try:
        with open(sys.argv[1]) as input_file:
            with open(sys.argv[2], "w") as out_file:
                for line in input_file:
                    current_median = get_median(len(list(set(line.split()))), current_median)
                    out_file.write('{0:.2f}'.format(current_median)+"\n")
    except IOError:
        print "output error"

if __name__ == "__main__":
    main()
