# CMPT 474 Spring 2014, Assignment 6
# Utilities for reading and writing tuples to the serverQ.py

# Core libraries
import json

# Libraries that must be installed via pip
import requests

# Libraries provided with assignment
from vectorclock import VectorClock

# Distinguished label to indicate a clock list
CLOCK_CODE = 'CLOCK_LIST_XXX'

class Queue(object):
    def __init__(self, port):
        self.port = port

    def get(self, channel):
        resp = requests.get('http://localhost:'+str(self.port)+'/q/'+channel,
                            headers={'content-type': 'application/json'})
        jresp = resp.json()
        if len(jresp) > 0:
            for k in jresp:
                if (isinstance(jresp[k],dict) and
                    jresp[k].keys() == [CLOCK_CODE]):
                    jresp[k] = [VectorClock.fromDict(dc) for dc in jresp[k][CLOCK_CODE]]
            return jresp
        else:
            return None
            
    def put(self, channel, dct):
        if not isinstance(dct, dict):
            raise Exception('Message to send is not a dict')
            
        for k in dct:
            if (isinstance(dct[k],(list, tuple)) and
                isinstance(dct[k][0], VectorClock)):
                dct[k] = {CLOCK_CODE: [vc.asDict() for vc in dct[k]]}
        
        res = requests.put('http://localhost:'+str(self.port)+'/q/'+channel,
                            data=json.dumps(dct),
                            headers={'content-type': 'application/json'})
                               
