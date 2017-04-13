#!/usr/bin/python
#coding=utf-8
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from confhelper import ConfigHelper

cfHelper = ConfigHelper()

def phoenix1(ms):
    #read from phoenix var high level api
    zk = cfHelper.phoenix1_zk
    topics = cfHelper.phoenix1_topics
    numThreads = cfHelper.phoenix1_num_threads
    group = cfHelper.phoenix1_groupid

    topicList = topics.split(',')
    topicMap = dict((i, numThreads) for i in topicList)
    kvs = KafkaUtils.createStream(ms.ssc
            ,zk
            ,group
            ,topicMap)

    lines = kvs.map(lambda x: x[1])
    return lines 

def phoenix2(ms):
    #read from phoenix var direct method
    topics = cfHelper.phoenix2_topics
    brokers = cfHelper.phoenix2_brokers
    group = cfHelper.phoenix2_groupid

    topicList = topics.split(',')
    params = {"metadata.broker.list": brokers,
            "auto.offset.reset": "largest",
            "group.id": group
            }
    fromOffset = createFromOffsetDict(ms)
    kvs = KafkaUtils.createDirectStream(ms.ssc
            ,topicList
            ,params
            ,fromOffsets=fromOffset)

    kvs.foreachRDD(lambda rdd: saveOffset(ms, rdd))
    lines = kvs.map(lambda x: x[1])
    return lines 

def socket(ms):
    print("deal data, put your code here")
    host = cfHelper.socket_host
    port = cfHelper.socket_port
    lines = ms.ssc.socketTextStream(host, int(port))
    return lines

def saveOffset(ms, rdd):
    ms.offsetRange = rdd.offsetRanges()
    for o in ms.offsetRange:
        print ('%s %s %s %s' % (o.topic, o.partition, o.fromOffset, o.untilOffset))

def createFromOffsetDict(ms):
    fromOffset = {}
    lines = []
    with open(ms.taskId, 'r') as f:
        lines = f.readlines()
    f.close()
    for line in lines:
        line = line.strip('\n')
        if not line:
            continue
        ww = line.split(' ')        
        topicPartion = TopicAndPartition(ww[0], int(ww[1]))
        fromOffset[topicPartion] = long(ww[3]) 
    return fromOffset

