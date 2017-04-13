#!/usr/bin/python
#coding=utf-8
import urllib2, urllib
from confhelper import ConfigHelper

def postTool(url, data):
    req = urllib2.Request(url)
    data = urllib.urlencode(data)
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
    response = opener.open(req, data)
    return response.read()

class BackCaller:
    def __init__(self, taskId, appId):
        self._taskId = taskId
        self._appId = appId
        self._params = []
        cfHelper = ConfigHelper()
        self._host = cfHelper.mustang_host
        self._port = cfHelper.mustang_port



    def onStart(self):
        self.setStatus('running')
        self.setAppId(self._appId)

    def onStop(self):
        self.setStatus('stopped')

    def onExpection(self):
        self.setStatus('exception')
        

    def setStatus(self, status):
        url = "http://%s:%s%s%s" % \
                (self._host, self._port, "/task/status/", self._taskId)
        params = {"status":status}
        postTool(url, params)

    def setAppId(self, appId):
        url = "http://%s:%s%s%s" % \
                (self._host, self._port, "/task/appid/", self._taskId)
        params = {"appid":appId}
        postTool(url, params)



