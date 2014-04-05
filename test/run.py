#!/usr/bin/env python
# coding=utf8

# CMPT 474, Spring 2014, Assignment 6 (tea-emporium-3) run file

# Core libraries
import math
import random
import string
import StringIO
import itertools
import functools

# Standard libraries for interacting with OS
import os
import time
import json
import shutil
import argparse
import urlparse
import subprocess

# Extend path to our containing directory, so we can import vectorclock
import sys
sys.path.append(sys.path[0]+'/..')

# Libraries that have to have been installed by pip
import redis
import requests
from termcolor import colored

# File distributed with assignment boilerplate
from vectorclock import VectorClock


class HTTPOutput():
    def __init__(self, url):
        self.url = url
    def write(self, data):
        requests.post(self.url, data=data, headers={ 'Content-type': 'application/json' })
    def flush(self):
        pass

parser = argparse.ArgumentParser(description='Process.')

parser.add_argument('--key',
                    dest='key',
                    action='store',
                    nargs='?',
                    default=''.join(random.sample((string.ascii_uppercase +
                                                   string.digits)*10, 10)),
                    help='random nonce')

parser.add_argument('--results',
                    dest='output',
                    action='store',
                    nargs='?',
                    default=None,
                    help='where to send results (default stdout); can be url or file')

parser.add_argument('--leavedb',
                    action='store_true',
                    help='leave the database after termination')

parser.add_argument('--test',
                    dest='test',
                    action='store',
                    nargs='?',
                    default='simple',
                    help='name of single test to run')

parser.add_argument('--ndb',
                    dest='ndb',
                    type=int,
                    action='store',
                    nargs='?',
                    default=None,
                    help='number of database nodes to spin up')

parser.add_argument('--wait',
                    dest='wait',
                    type=int,
                    action='store',
                    nargs='?',
                    default=1,
                    help='number of seconds to wait for servers to start')

args = parser.parse_args()
if args.output:
    url = urlparse.urlparse(args.output)
    if not url.scheme:
        output = file(url.path, 'w')
    else:
        output = HTTPOutput(urlparse.urlunparse(url))
else:
    output = sys.stdout

# Seed the random number generator with a known value
random.seed(args.key)

ITEM = 'zoo'

if args.ndb:
    ndb = args.ndb
else:
    ndb = 4 # Number of database nodes
nlb = 1 # Number of load balancers
nq = 1 # Number of queue servers
digest_length = 2 # Number of PUTs in each digest

# Ports for the services
lb_base = 2500 # Base port number for load balancers
db_base = 3000 # Base port number for database nodes
rd_base = 5555 # Base port number for redis servers
qs_base = 6000 # Base port number for queue servers

base = os.path.dirname(os.path.abspath(os.path.join(__file__, '..')))
log = os.path.join(base, 'var', 'log')
db = os.path.join(base, 'var', 'db')

if os.path.exists(log): shutil.rmtree(log)
if os.path.exists(db): shutil.rmtree(db)

os.makedirs(log)
os.makedirs(db)

rd_configs = [ { 'id': str(i), 'host': 'localhost', 'rd-port': rd_base+i } for i in range(ndb) ]
rd_processes =  [ subprocess.Popen(['redis-server',
                                '--port', str(config['rd-port']),
                                '--bind', '127.0.0.1',
                                '--logfile', os.path.join(log, 'server'+config['id']+'.log'),
                                '--dbfilename', 'server'+config['id']+'.rdb',
                                '--databases', '1',
                                '--dir', db ])
                                for config in rd_configs ]
clients = [ redis.StrictRedis(host=config['host'], port=config['rd-port'], db=0) for config in rd_configs ]

lb_configs = [ {'id': i,
                'port': lb_base+i,
                'ndb': ndb,
                'db-base-port': db_base} for i in range(nlb) ]
lb_servers = [ subprocess.Popen(['python', os.path.join(base, 'serverLB.py'), json.dumps(config)]) for config in lb_configs ]

qs_configs = [ {'id': i, 'port': qs_base+i, 'nq': nq, 'ndb': ndb} for i in range(nq) ]
qs_servers = [ subprocess.Popen(['python', os.path.join(base, 'serverQ.py'), json.dumps(config)]) for config in qs_configs ]

db_configs = [ {'id': i,
                'servers':[{'host': 'localhost', 'port': rd_base+i}],
                'hostport': db_base+i,
                'ndb': ndb,
                'baseDBport': db_base,
                'qport': qs_base,
                'digest-length': digest_length}
                for i in range(ndb)]
db_servers = [ subprocess.Popen(['python', os.path.join(base, 'serverDB.py'), json.dumps(config)]) for config in db_configs ]

def endpoint(id, lb_port):
    return 'http://localhost:'+str(lb_port)+'/rating/'+id

def get(id, ec=False, port=lb_base):
    """ Get a value.

        By default, this will issue a strongly consistent read to the
        load balancer. Setting ec=True will request an eventually
        consistent read. Setting port to the port of a DB instance
        does a direct get to that instance, bypassing the load balancer.
    """
    headers = { 'Accept': 'application/json' }
    url = endpoint(id, port)
    try:
        if ec:
            response = requests.get(url, headers=headers, params={'consistency': 'weak'})
        else:
            response = requests.get(url, headers=headers)
    except Exception as e:
        raise Exception("Invalid request: url %s, exception %s" % (url, e))
    try:
        data = response.json()
    except:
        raise Exception('Unexpected response: %s HTTP %d  %s' % (url, response.status_code, response.text))

    try:
        rating = float(data['rating'])
    except:
        rating = data['rating']

    choices = data['choices']
    #TODO: Handle return of malformed vector clock
    clocks = data['clocks']
    return rating, choices, [VectorClock.fromDict(vcstr) for vcstr in clocks]

def put(id, rating, clock, port=lb_base):
    headers = { 'Accept': 'application/json', 'Content-type': 'application/json' }
    data = json.dumps({ 'rating': rating, 'clock': clock.clock })
    resp = requests.put(endpoint(id, port), headers=headers, data=data)

def result(r):
    output.write(json.dumps(r)+'\n')
    output.flush()

def testResult(result, rgot, rexp, choicesgot, choicesexp, clocksgot, clocksexp):
    result({ 'type': 'EXPECT_RATING', 'got': rgot, 'expected': rexp})
    result({ 'type': 'EXPECT_CHOICES', 'got': choicesgot, 'expected': choicesexp })
    result({ 'type': 'EXPECT_CLOCKS', 'got': [c.asDict() for c in clocksgot], 'expected' : [c.asDict() for c in clocksexp] })

def getAndTest(item, rexp, choicesexp, clocksexp):
    r, ch, cl = get(item)
    testResult(r, rexp, ch, choicesexp, cl, clocksexp)

def makeVC(cl, count):
    return VectorClock().update(cl, count)

def info(msg):
    sys.stdout.write(colored('ℹ', 'green')+' '+msg+'\n')
    sys.stdout.flush()

def flush():
    for client in clients:
        client.flushall()

def count():
    return sum(map(lambda c:c.info()['total_commands_processed'],clients))

def sum(l):
    return reduce(lambda s,a: s+a, l, float(0))

def mean(l):
    return sum(l)/len(l)

def variance(l):
    m = mean(l)
    return map(lambda x: (x - m)**2, l)

def stddev(l):
    return math.sqrt(mean(variance(l)))

def usage():
    def u(i):
        return i['db0']['keys'] if 'db0' in i else 0
    return [ u(c.info()) for c in clients ]


print("Running test #"+args.key)

# Some general information
result({ 'name': 'info', 'type': 'KEY', 'value': args.key })
result({ 'name': 'info', 'type': 'SHARD_COUNT', 'value': ndb })

# Give the server some time to start up
time.sleep(1)

tests = [ ]
def test():
    def wrapper(f):
        def rx(obj):
            x = obj.copy()
            obj['name'] = f.__name__
            result(obj)
        @functools.wraps(f)
        def wrapped(*a):
            info("Running test %s" % (f.__name__))
            # Clean the database before subsequent tests
            flush()
            # Reset the RNG to a known value
            random.seed(args.key+'/'+f.__name__)
            f(rx, *a)
        tests.append(wrapped)
        return wrapped
    return wrapper

@test()
def simple(result):
    """ Simple write to empty item should be unique. """
    rating  = 5
    time = 1
    cv = VectorClock().update('c0', time)
    put(ITEM, rating, cv)
    r, choices, clocks = get(ITEM)
    testResult(result, r, rating, choices, [rating], clocks, [cv])

@test()
def testGetGossip(result):
    """ Check that a get forces a gossip. 
        This test works regardless of the LB's hash algorithm because
        the test directly gets from the DB instance, forcing it to merge
        any pending gossip.
    """
    if ndb <= 1:
        result({'type': 'TEST_SKIPPED', 'reason': 'Only 1 database (no gossip)'})

    base = 'aardvark'
    first = base+'0'
    cv = VectorClock().update('c0', 1)
    put(first, 1, cv)

    for i, cl in enumerate(clients):
        if cl.exists('/rating/'+first): break
    else:
        result({'type': 'KEY_NOT_SAVED'})
        return
    baseclient = db_base+i
    basesub = db_base + (i+1)%ndb

    for i in range(1,digest_length+1):
        item = base + str(i)
        put(item, 1, cv, baseclient)

    result({'type': 'str', 'expected': 'False', 'got': str(clients[1].exists(base+'*'))})

    rating, _, _ = get('hello', port=basesub)
    result({'type': 'float', 'expected': 0.0, 'got': rating})

    rating, choices, clocks = get(base+'0', port=basesub)
    result({'type': 'float', 'expected': 1.0, 'got': rating})
    result({'type': 'EXPECT_CHOICES', 'expected': [1.0], 'got': choices})
    result({'type': 'EXPECT_CLOCKS', 'expected': [cv.asDict()], 'got': [c.asDict() for c in clocks]})

# Go through all the tests and run them
try:
    for test in tests:
        if args.test == None or args.test == test.__name__:
            test()
finally:
    # Shut. down. everything.
    for lb_server in lb_servers: lb_server.terminate()
    for db_server in db_servers: db_server.terminate()
    for qs_server in qs_servers: qs_server.terminate()
    if not args.leavedb:
        for p in rd_processes: p.terminate()

# Fin.
