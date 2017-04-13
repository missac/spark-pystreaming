#!/usr/bin/python
#coding=utf-8

from confhelper import ConfigHelper

def makeConf(out):
    if out == 'eagles':
        makeEsConf()
    elif out == 'stork':
        makeStorkConf()
    elif out == 'console':
        return
    else:
        raise Exception("Invalid output", out)

es_write_conf = {
        "es.nodes" : "localhost",
        "es.port" : "9200",
        "es.resource" : "titanic/value_counts"
        } 

def makeEsConf():
    cfHepler = ConfigHelper()
    node = cfHepler.es_host
    port = cfHepler.es_port
    table = cfHepler.es_table
    es_write_conf['es.node'] = node
    es_write_conf['es.port'] = port
    es_write_conf['es.resource'] = table

def toEagles(rdd):
    rdd.saveAsNewAPIHadoopFile(
            path='-', 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=es_write_conf)

#stork conf
pg_properties = {
    "user": "postgres",
    "password": "postgres"
    }

pg_url = ''

table = ''

mode = 'append'

def makeStorkConf():
    global pg_url, table, mode, pg_properties
    cfHelper = ConfigHelper()
    host = cfHelper.stork_host
    port = cfHelper.stork_port
    user = cfHelper.stork_user
    passwd = cfHelper.stork_passwd
    database = cfHelper.stork_db
    table = cfHelper.stork_table
    #mode = cfHelper.mode

    pg_properties['user'] = user 
    pg_properties['password'] = passwd
    pg_url = 'jdbc:postgresql://%s:%s/%s' % (host, port, database)

#.option("dbtable", "select * from test limit 1") \
def toStork(df):
        df.write.jdbc(url=pg_url, table=table, mode='append', properties=pg_properties)

def getStorkColumns(spark):
    df = spark.read \
            .format("jdbc") \
            .option("url", pg_url)\
	    .option("dbtable", "(select * from test limit 1) t1") \
            .option("user", pg_properties["user"])\
            .option("password",pg_properties["password"] ) \
            .option("fetchsize", "10") \
            .load()
    
    return df.columns
