
Insight Data Engineering Coding Challenge
===================================

The code for both the [challenges](https://github.com/InsightDataScience/cc-example) has been implemented **Python** with the below environment configurations:
**Python Version:** 2.7.6
**OS:** Mac OSX, Ubuntu 14.04



#### **Contents of run.sh** (Primary Submission)
- Challenge 1 solution: **_tweet_word_count.py_**
- Challenge 2 Solution: **_tweet_rolling_median_bisect.py_**

#### **Steps to run:** `sh run.sh or ./run.sh`
Both the challenges are also solved using  **Apache Spark** with **PySpark**. A single IPython notebook named [**PySpark_Insight_Data_Engineering_Challenge.ipynb**](http://nbviewer.ipython.org/github/suhashm/Insight_Coding_Challenge/blob/master/src/PySpark_Insight_Data_Engineering_Challenge.ipynb) with PySpark implementation  for both the challenges is included in the src directory.

The code is appropriately commented and the [**PEP08**<sup>1</sup>](#references) coding style for python has been followed.

----------


###**Table of Contents**

- [Insight Data Engineering Coding Challenge](#insight-data-engineering-coding-challenge )
	- [Challenge 1: Tweet Word Count](#challenge-1-tweet-word-count)
	- [Challenge 2: Rolling median of tweets](#challenge-2-rolling-median-of-tweets)
				- [1. Sliding window approach](#1-sliding-window-approach)
				- [2. Heaps](#2-heaps)
				- [3. Using numpy](#3-using-numpy)
			- [Evaluation](#evaluation)
			- [Conclusion](#conclusion)
	- [Apache Spark - PySpark implementation](#apache-spark-pyspark-implementation)
	- [References](#references)


----------

##Challenge 1: Tweet Word Count
#### <i class="icon-pencil"></i> Implementation Details

For each of the tweets from input tweet file, frequency of the word is calculated using a **python dictionary (defaultdict)** as the data structure.

**File:** tweet_word_count.py
**To execute:**
```python
python src/tweet_word_count.py tweet_input/tweets.txt tweet_output/ft1.txt
```
> **Note:**
> The use of a dictionary provides accurate results to calculate the cardinality of the words. Though, the look up time is very fast **O(1)**,  the size of the dictionary will be equal to that of number of unique words in all the tweets.
> In order to scale the cardinality process, I have also explored various probabilistic models like **Linear Probabilistic Counter,  [Count-Min Sketch <sup>2</sup>](#references), [HyperLogLog(HLL) <sup>3</sup>](#references)**. However, HLL provides only the overall cardinality of the unique words, but we need to extract word frequency for all the words in the tweet.
>[**Scalable Bloom filters** <sup>4</sup>](#references) are another promising option, which use very less memory and ensures zero false negatives. However, bloom filters are mainly used to check the membership of an object. Since our use case is to find out the cardinality, I proceeded with using the dictionary approach.

----------

## Challenge 2: Rolling median of tweets

#### <i class="icon-pencil"></i> Implementation Details
I have implemented 3 different solutions to compute rolling median:

#####1. **Sliding window approach**
*(submitted solution for **run.sh**)*

- With deque and bisect library
	 - The idea here is to maintain the sorted data as new elements are added and old one removed as a sliding window advances over a stream of data.
	 - The use of deque enables very fast access times for insert and remove operations.
	 -  The bisect module helps in maintaining the list in sorted order without having to explicitly sort the list after each insertion.
	 - This approach scales very well for long lists of items with expensive comparison operations and it takes **less than 2 seconds** to compute the rolling median for **120K tweets.**
	 - The current  approach uses a window size equal to number of tweets in the input file. This can be changed to work on streaming data by incrementing the window size  with each tweet.
	 - The implementation can also be modified load the data as per memory constraint and read rest of the input line by line. This is also verified and the code scales very well for huge datasets.
	 - This approach can also be extended to compute statistics efficiently for time-series analysis, as the window size can be configured in the program.

**File:** tweet_rolling_median_bisect.py
**To execute:**
```python
python src/tweet_rolling_median_bisect.py tweet_input/tweets.txt tweet_output/ft2.txt
```


> **Note:**
> The program is inspired by [**"Efficient Algorithm for computing a Running Median"** <sup>5</sup>](#references) research paper.



##### **2. Heaps**
- Two Heaps, one MaxHeap and one MinHeap is maintained.
-  The Max Heap is used to represent elements that are less than effective median and the Min Heap is for those elements that are greater than the effective median.
- Each element is processed one by one and the size of the heaps differ by utmost 1.
- When both heaps contain the same number of elements, the median is calculated as the average of root from both the heaps.
- If the heaps are unbalanced, the effective median is selected as the root of the heap consisting of more elements.

**File:** tweet_rolling_median_heap.py
**To execute:**
```python
python src/tweet_rolling_median_heap.py tweet_input/tweets.txt tweet_output/ft2.txt
```


> **Note:**
> This program gives accurate results, however the cost of insertion into a heap is O(log N) and for N tweets. Therefore, the overall time complexity is **O(N logN)**.  This Heap logic scales well, but not as much as the above sliding window approach.


##### **3. Using numpy**

> **Note:**
> To run this program, an external python library named  **numpy** is used. This can be installed using `sudo apt-get install python-numpy`

- numpy arrays are used to calculate the median of the tweets.
- For each tweet, the count of unique tweet words per tweet are added to the numpy array and median is calculated.
- Though this approach gives result in fairly quick time (~1 mins for 120K tweets) it is not completely scalable. This is because, whenever an element is inserted to the numpy array, a new array is created and the default numpy Quicksort operation takes **O(N logN)** time.


**File:** tweet_rolling_median_numpy.py
**To execute:**
```python
python src/tweet_rolling_median_numpy.py tweet_input/tweets.txt tweet_output/ft2.txt
```
#### Evaluation

The time taken to complete the calculation of rolling median for each of the approaches is shown below:  _(Mac OSX, 8 GB RAM, Dual core - i3)_

| Approach  | Time taken |
| ------------- | ------------- |
| Sliding Window  | 2.7 seconds  |
| numpy  | 100.4 seconds  |
| Heaps  | 181 seconds  |

#### Conclusion
From the above evaluation, it is clear that  the **Sliding window approach** using deque and bisect outperforms both numpy and heap implementations.
Hence,  the Sliding window approach can be considered as a scalable solution for calculating the running median of streaming tweets.


----------


Apache Spark - PySpark implementation
-----------------------------------------------

Both the challenges have also been solved using PySpark as an alternate implementation.

- Challenge 1 is solved using creating RDD's from reading the text file and then applying series of transformations like **flatMap, map, reduceByKey and sortByKey.**

- Challenge 2 is solved using the above mentioned Heap approach. In this approach, the transfer of variables from driver program to worker nodes are reduced using **broadcast variables.**

**File:** [PySpark_Insight_Data_Engineering_Challenge.ipynb](http://nbviewer.ipython.org/github/suhashm/Insight_Coding_Challenge/blob/master/src/PySpark_Insight_Data_Engineering_Challenge.ipynb)



References
-------------
1. [PEP08](https://www.python.org/dev/peps/pep-0008/) gives coding conventions for the Python code comprising the standard library in the main Python distribution.

2. [Cormode, Graham, and S. Muthukrishnan. "An improved data stream summary: the count-min sketch and its applications." Journal of Algorithms 55.1 (2005): 58-75.](http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)

3. [Flajolet, Philippe, et al. "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm." DMTCS Proceedings 1 (2008).](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)

4. [Almeida, Paulo Sérgio, et al. "Scalable bloom filters." Information Processing Letters 101.6 (2007): 255-261.](http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf)

5. [Mohanty, Soumya D. Efficient Algorithm for computing a Running Median. T030168-00-D. http://www. ligo. caltech. edu/docs, 2003.](https://dcc.ligo.org/public/0027/T030168/000/T030168-00.pdf)
