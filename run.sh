#!/usr/bin/env bash

# Challenge 1 - Script to run the tweet word count program
python ./src/tweet_word_count.py ./tweet_input/tweets.txt ./tweet_output/ft1.txt

# Challenge 2 - Script to calculate the rolling median of tweets
python ./src/tweet_rolling_median_bisect.py ./tweet_input/tweets.txt ./tweet_output/ft2.txt
