# Distributed-BloomFilter

## Introduction
This project presents a solution for implementing a distributed Bloom Filter using the map-reduce approach on Hadoop and Spark frameworks. The data used to create and populate the Bloom Filters comes from an IMDb dataset on movie ratings, with ten Bloom Filters being created to correspond with the IMDb rating scale of 1-10.

## What is a Bloom Filter?
A Bloom Filter is a space-efficient probabilistic data structure used to test if an element is a member of a set or not. It is important to note that while false positive matches are possible, false negatives are not. In this context, this type of data structure can be used to speed up the response time of search queries related to movies.

## How it works
The project consists of three main stages for the implementation of the Bloom Filters:

- Parameter Calculation: In this stage, the dimensional parameters for each Bloom Filter are calculated.
- Bloom Filter Creation: In this stage, the Bloom Filters are created and populated using the dataset.
- Bloom Filter Validation: In this stage, the performance of the data structures, in terms of false positive rate, are evaluated.

## Benefits
The use of a distributed Bloom Filter in this project can bring several benefits, such as:

- Improved response time for search queries related to movies.
- Space-efficient data structure.
- False negatives are not possible, reducing the risk of errors in the results.

## Conclusion
This project presents a solution for implementing a distributed Bloom Filter using the map-reduce approach on Hadoop and Spark frameworks. The data used to create and populate the Bloom Filters comes from an IMDb dataset on movie ratings, with ten Bloom Filters being created to correspond with the IMDb rating scale of 1-10. The use of a distributed Bloom Filter can bring several benefits such as improved response time for search queries, space-efficiency and less risk of errors in the results.
