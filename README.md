#TDD Mapreduce Aggregate Example 
This is a basic [Hadoop](http://hadoop.apache.org/) aggregate example that builds with [Maven](http://maven.apache.org/) and uses [MRUnit](http://incubator.apache.org/projects/mrunit.html) for unit testing.

[![Build Status](https://travis-ci.org/mmcc007/MapReduceAggregate.svg?branch=master&style=flat-square)](https://travis-ci.org/mmcc007/MapReduceAggregate)

To run tests and build jar do

    mvn install

You can then run the jar file on your hadoop cluster.

The main point is to demonstrate a use case that is hard to find online, but is common in practice. An aggregate MapReduce is synonymous to a 'groupBy' in a select statement. 

This aggregate feature is synonymous with SQL tools that resolve to this kind of mapreduce instance.
