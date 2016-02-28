# Scala-Reactive-Programming
This repository holds the coursework for the Coursera course on [reactive programming in Scala](https://class.coursera.org/reactive-002).

I'm most proud of the work I did for assignment 6: making a simple replicated key-value store using actors. When I took this Coursera course, I was working for Teradata on several Hadoop projects. The simple replicated key-value store used a lot of the same concepts (and akka framework) that power many of the components of Hadoop. It was fascinating learning how actors and messages can simplify the task of building high-performing and resilient distributed systems. In addition I genuinely enjoy working with actors and messaging systems. They are relatively easy to understand and provide an elegant solution to many of the problems associated with building efficient distributed systems.

Overall this was a great course and I highly recommend it. I struggled a bit with the sections on futures and promises but found that the concepts of actors and messages came very easily to me.

## Assignments

### Week 2 - Calculator

This assignment focused around working with Signals and was composed of 3 main parts: 

1. Dynamic character counting of text in a html form (to mimic a simplified version of twitter's character counting)
2. Dynamic square root solver
3. Spreadsheet-esque calculator (the meat of the assignment)

### Week 3 - NodeScala

The week 3 assignment focused around working with (and extending) futures. The goal of the assignment was to implement a simple asynchronous HTTP server.

### Week 4 - Suggestions

This assignment was all about using observables to create a reactive GUI. In this case, the GUI was used to search and display wikipedia articles.

### Week 5 - ActorBinTree

The goal of this assignment was to implement a binary tree using actors and messages. The binary tree was unbalanced and used a garbage collection mechanism to remove elements. 

### Week 6 - KVStore

This assignment was my favorite assignment of the course. It focused on using actors to implement a simple replicated key-value store where persisting changes to disk and communicating with replicas were prone to failure.
