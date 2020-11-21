## MapReduce-WC-Alternatives

This repository demonstrates implementation of Hadoop Map-Reduce programs with different alternatives.

#### Part 1
This part contains a MapReduce program to count number of short words (1-4 letters), medium words (5-7 letters) words, long words (8-10 letters) and extra-long words (More than 10 letters).

#### Part 2
This part contains a MapReduce program that outputs a count of all words that begin with a vowel and a count of all words that begin with a consonant.

#### Part 3 (In-mapper combining)
This part contains a MapReduce program to count the number of each word where the in-mapper combining is implemented rather than an independent combiner.

#### Part 4 (Partitioner)
This program extends the first part such that the
- short words (1-4 letters) and extra-long words (More than 10 letters) are processed in one reducer,
- medium words (5-7 letters) and long words (8-10 letters) are processed in another reducer.
