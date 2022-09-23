# Distributed-BloomFilter

In this project we present a possible solution for the implementation of a distributed Bloom Filter using the map reduce approach on Hadoop and Spark framework. The data used to create and populate these Bloom Filter came from an IMDb dataset on average rating related to movies. Ten Bloom Filter will be realized since the IMDb rating is a measure that goes from 1 to 10

The Bloom Filter is a space-efficient probabilistic data structure used to test if an element is a member of a set or not. False positive matches are possible but false negatives not. In this context, this type of data structure can be used, for instance, to speed up the response time of search query on movies. 

The main components are Bloom Filter type objects on which will be applied 3 Map-Reduce stages for their realization. 
The first one is the ParameterCalculation in which the dimensional parameters for each Bloom Filter are calculated; after this follows the BloomFilterCreation phase in which Bloom Filters are created and populated using the dataset. Finally, the BloomFilterValidation phase in which the performance of the data structures, in terms of false positive rate, are evaluated.
