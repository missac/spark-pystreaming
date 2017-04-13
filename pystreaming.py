#!/usr/bin/python
#coding=utf-8

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.sql import SQLContext
from signal import signal, SIGTERM
from sys import exit
from confhelper import ConfigHelper
from callbacker import BackCaller
import atexit
import source
import output
import process

class MustangStream():
    def __init__(self):

        conf = SparkConf()
        conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

        self.sc = SparkContext(conf=conf)
        self.sqlctx = SQLContext(self.sc)
        #change the interval if u need
        self.sparkSession = SparkSession\
                .builder\
                .config(conf=SparkConf())\
                .getOrCreate()

        #load conf 
        cfHelper = ConfigHelper()
        self.checkPointDir = cfHelper.checkpoint_dir
        self.interval = cfHelper.deal_interval
        self.source = cfHelper.data_source
        self.out = cfHelper.data_output
        self.taskId = conf.get("spark.task.id")
        self.isTest = conf.get("spark.task.istest")
        if self.isTest == '':
            self.isTest = 'false'
        self.offset = []

        #init output
        output.makeConf(self.out)
        if self.out == 'stork':
            print(output.pg_url)
            self.storkRows = output.getStorkColumns(self.sparkSession)

        self.appId = self.sc.applicationId 
        self.backCaller = BackCaller(self.taskId, self.appId)
        if self.checkPointDir == 'notset':
            self.ssc = StreamingContext(self.sc, self.interval)
            self.dealData()
        else:
            self.ssc = StreamingContext. \
                    getOrCreate(self.checkPointDir, self.createStreamingCtx())


    def createStreamingCtx(self):
        #conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
        ssc = StreamingContext(self.sc, self.interval)
        ssc.checkpoint(self.checkPointDir)
        self.dealData()
        return ssc

    def onStart(self):
        cmd = "streaming task %s is start" % self.taskId 
        print(cmd)
        #self.backCaller.onStart()

    def onStop(self):
        print("spark streaming is stopped")
        self.saveOffset()
        #self.backCaller.onStop()

    def onException(self):
        print("spark streaming is stopped")
        self.saveOffset()

    def saveOffset(self):
        if not self.offsetRange:
            return
        fileName = self.taskId
        f=open(fileName,'w') 
        for o in self.offsetRange:
            msg = ('%s %s %s %s\n' % (o.topic, o.partition, o.fromOffset, o.untilOffset))
            f.write(msg)
        f.close()
    
    def run(self):
        #self.dealData()
        self.ssc.start()
        #self.ssc.awaitTerminationOrTimeout()
        self.ssc.awaitTermination()

    def dealData(self):
        try:
            data = None
            if self.dataSource == 'phoenix1':
                data = source.phoenix1(self)
            elif self.dataSource == 'phoenix2':
                data = source.phoenix2(self) 
            elif self.dataSource == 'socket':
                data = source.socket(self)
            else:
                raise Exception("Invalid source", dataSource)
            res = process.process(self, data)
            if self.isTest == 'true':
                res.pprint()
                self.saveData(res)
            else:
                self.saveData(res)
        except Exception,ex:
            print(str(ex))
            raise

    def saveData(self, dstream):
        #dstream.pprint()
        if self.oput == 'eagles':
            dstream.foreachRDD(lambda rdd: self.saveToEagles(rdd))
        elif self.out == 'stork':
            dstream.foreachRDD(lambda rdd: self.saveToStork(rdd))
        elif self.out == 'redis':
            dstream.pprint()
        elif self.out == 'console':
            dstream.pprint()
        else:
            dstream.pprint()
         
    def saveToStork(self, rdd):
        if rdd.isEmpty():
            print("empty")
            return
        df = self.sqlctx.createDataFrame(rdd, self.storkRows) 
        output.toStork(df)

    def saveToEagles(self, rdd):
        if rdd.isEmpty():
            return
        output.toEagles(rdd)


if __name__ == "__main__":
    try:
        ns = MustangStream()
        atexit.register(ns.onStop)
        signal(SIGTERM, lambda signum, stack_frame: exit(1))
        ns.run()
    except Exception,ex:
        print(str(ex))
        #ns.onException()

