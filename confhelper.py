#!/usr/bin/python
#coding=utf-8

import ConfigParser

mustang_conf = '/opt/dana/spark/mzt/mustang.ini'
task_conf = '/opt/dana/spark/mzt2/python/conf.ini'

class ConfigHelper(object):  
    def __new__(cls, *args, **kw):  
        if not hasattr(cls, '_instance'):  
            orig = super(ConfigHelper, cls)  
            cls._instance = orig.__new__(cls, *args, **kw)  

            cf = ConfigParser.ConfigParser()
            cf.read(mustang_conf)
            cls.mustang_host = cf.get('mustang', 'host')
            cls.mustang_port = cf.get('mustang', 'port')

            cf.read(task_conf)

            #read task conf
            cls.checkpoint_dir = cf.get('task', 'checkpoint_dir')
            cls.deal_interval = cf.get('task', 'deal_interval')
            cls.data_source = cf.get('task', 'data_source')
            cls.data_output = cf.get('task', 'data_output')

            #read eagles
            cls.es_host = cf.get('eagles', 'eagles_host')
            cls.es_port = cf.get('eagles', 'eagles_port')
            cls.es_table = cf.get('eagles', 'eagles_table')
            
            #read stork
            cls.stork_host = cf.get('stork', 'stork_host') 
            cls.stork_port = cf.get('stork', 'stork_port') 
            cls.stork_user = cf.get('stork', 'stork_user') 
            cls.stork_passwd = cf.get('stork', 'stork_passwd') 
            cls.stork_db = cf.get('stork', 'stork_db')
            cls.stork_table = cf.get('stork', 'stork_table')
            cls.mode= cf.get('stork', 'stork_mode') 

            #read phoenix1
            cls.phoenix1_zk = cf.get('phoenix1', 'zk')
            cls.phoenix1_topics = cf.get('phoenix1', 'topics')
            cls.phoenix1_num_threads = cf.get('phoenix1', 'num_threads')
            cls.phoenix1_groupid = cf.get('phoenix1', 'group_id')
            #read phoenix2
            cls.phoenix2_brokers = cf.get('phoenix2', 'brokers')
            cls.phoenix2_topics = cf.get('phoenix2', 'topics')
            cls.phoenix2_groupid = cf.get('phoenix2', 'group_id')
            cls.phoenix2_offset_reset  = cf.get('phoenix2', 'offset_reset')
            #read socket
            cls.socket_host = cf.get('socket', 'socket_host')
            cls.socket_port = cf.get('socket', 'socket_port')


        return cls._instance  

