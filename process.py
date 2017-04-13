#!/usr/bin/python
#coding=utf-8

#input: A DStream object
#output: transformed DStream 
def process(ms, data):
    lines = data.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" "))\
                .map(lambda word: (word, 1))\
                .reduceByKey(lambda a, b: a+b)
    return counts



