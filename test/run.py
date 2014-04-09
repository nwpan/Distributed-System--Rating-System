#!/usr/bin/env python
# coding=utf8

# CMPT 474, Spring 2014, Assignment 6 (tea-emporium-3) run file

# Core libraries
import math
import copy
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

parser = argparse.ArgumentParser(description='Run single test or test suite.')

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
                    help='leave the Redis servers running after termination')

parser.add_argument('--test',
                    dest='test',
                    action='store',
                    nargs='?',
                    default=None,
                    help='name of single test to run')

parser.add_argument('--ndb',
                    dest='ndb',
                    type=int,
                    action='store',
                    nargs='?',
                    default=4,
                    help='number of database nodes to spin up; default %(default)s')

parser.add_argument('--wait',
                    dest='wait',
                    type=int,
                    action='store',
                    nargs='?',
                    default=1,
                    help=('number of seconds to wait for servers to start; '
                          'occurs at start of every test; default %(default)s'))

args = parser.parse_args()
if args.output:
    url = urlparse.urlparse(args.output)
    if not url.scheme:
        output = file(url.path, 'w')
    else:
        output = HTTPOutput(urlparse.urlunparse(url))
else:
    output = sys.stdout

# Import the list of things to rate
base = os.path.dirname(os.path.abspath(os.path.join(__file__, '..')))
entities = open(os.path.join(base, 'test', 'entities.txt')).read().splitlines()

# Seed the random number generator with a known value
random.seed(args.key)

ITEM = 'zoo'

ndb = args.ndb # Number of database nodes
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
                'logfile': os.path.join(log, 'serverLB'+str(i)+'.log'),
                'port': lb_base+i,
                'ndb': ndb,
                'db-base-port': db_base} for i in range(nlb) ]
lb_servers = [ subprocess.Popen(['python', os.path.join(base, 'serverLB.py'), json.dumps(config)]) for config in lb_configs ]

qs_configs = [ {'id': i, 'port': qs_base+i, 'nq': nq, 'ndb': ndb} for i in range(nq) ]
qs_servers = [ subprocess.Popen(['python', os.path.join(base, 'serverQ.py'), json.dumps(config)]) for config in qs_configs ]

db_configs = [ {'id': i,
                'logfile': os.path.join(log, 'serverDB'+str(i)+'.log'),
                'servers':[{'host': 'localhost', 'port': rd_base+i}],
                'hostport': db_base+i,
                'ndb': ndb,
                'baseDBport': db_base,
                'qport': qs_base,
                'digest-length': digest_length}
                for i in range(ndb)]
dbTestConfigs = {} # Specialized db_configs for specific tests
db_servers = []

def mkKey(entity):
    """ Return the Redis key for a given entity.  """
    return '/rating/'+entity

def endpoint(id, port):
    return 'http://localhost:'+str(port)+mkKey(id)

def getDBId(entity):
    """ Return the id of the Redis server in which this entity was stored.

        This assumes that there has been exactly one write of this key
        and no gossip has been induced since then.
    """
    for i, cl in enumerate(clients):
        if cl.exists(mkKey(entity)): break
    else:
        return -1
    return i

def dbSubscriber(id):
    """ Return the id of the DB server that subscribes to id's channel. """
    return (id+1) % ndb

def dbPublisher(id):
    """ Return the id of the publisher of the channel that id subscribes. """
    return (id-1) % ndb

def dbPort(id):
    """ Return the port number for a DB id. """
    return db_base+id

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

def testResult(result, rgot, rexp, choicesgot, choicesexp, clocksgot, clocksexp, entity=None):
    if entity == None:
        result({ 'type': 'EXPECT_RATING', 'got': rgot, 'expected': rexp})
        result({ 'type': 'EXPECT_CHOICES', 'got': choicesgot, 'expected': choicesexp })
        result({ 'type': 'EXPECT_CLOCKS', 'got': [c.asDict() for c in clocksgot], 'expected' : [c.asDict() for c in clocksexp] })
    else:
        result({ 'type': 'EXPECT_RATING', 'got': rgot, 'expected': rexp, 'entity': entity})
        result({ 'type': 'EXPECT_CHOICES', 'got': choicesgot, 'expected': choicesexp, 'entity': entity })
        result({ 'type': 'EXPECT_CLOCKS', 'got': [c.asDict() for c in clocksgot], 'expected' : [c.asDict() for c in clocksexp], 'entity': entity })

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

def restartDBServers(testName):
    stopDBServers()
    if len(db_servers) > 0: # Only drain queue if a test has been run
        res = requests.delete('http://localhost:'+str(qs_base)+'/clear')
    startDBServers(testName)

def startDBServers(testName):
    global db_servers
    if testName in dbTestConfigs:
        configs = dbTestConfigs[testName]
    else:
        configs = db_configs
    db_servers = [ subprocess.Popen(['python',
                                     os.path.join(base, 'serverDB.py'),
                                     json.dumps(config)])
                                     for config in configs ]
    # Give the server some time to start up
    time.sleep(args.wait)


def stopDBServers():
    if len(db_servers) > 0:
        for db_server in db_servers: db_server.terminate()

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
            restartDBServers(f.__name__)
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
def simpleEv(result):
    """ Test eventually consistent read.
    
        Do a simple write followed by both eventually- and strongly- 
        consistent reads.

        Implementations of the load balancer are permitted to route
        1/N of the requests to the primary DB instance. This test
        makes a large number of calls to LB and considers the test
        passed if the number of strongly consistent reads (up to
        date results) is in the 99% zone of the binomial distribution.
    """
    rating = 5
    time = 1
    cv = VectorClock().update('c0', time)
    put(ITEM, rating, cv)
    latestVal = 0
    for i in range(200):
        ecr, ecchoices, ecclocks = get(ITEM, ec=True)
        if int(ecr) == 0: latestVal += 1
    # Binomial critical value: 200 trials, 0.75 prob, 1% => 135 or more
    result({'type': 'GE', 'got': latestVal, 'expected': 135})
    r, choices, clocks = get(ITEM)
    testResult(result, r, rating, choices, [rating], clocks, [cv])

@test()
def discardOlderRating(result):
    """ Attempt to write an out-of-date rating. """
    entity = 'capital-of-heaven-keemun-black-tea'
    ratRecent = 1
    vcRecent = VectorClock().update('c12', 20)
    ratOld = 5
    vcOld = VectorClock().update('c12', 19)

    put(entity, ratRecent, vcRecent)
    put(entity, ratOld, vcOld)
    r, ch, cl = get(entity)
    testResult(result, r, ratRecent, ch, [ratRecent], cl, [vcRecent])

@test()
def testDBUnit(result):
    """ Test basic operations of DB directly. """
    entity = 'fruta-bomba-green-tea'
    dbid = 2
    dbp = dbPort(dbid)
    # Read on empty DB should return 0
    rating, choices, clocks = get(entity, port=dbp)
    result({'type': 'float', 'expected': 0.0, 'got': rating})

    # After write, read should return latest value
    put(entity, 1, VectorClock().update('c0', 1), port=dbp)
    rating, choices, clocks = get(entity, port=dbp)
    result({'type': 'float', 'expected': 1.0, 'got': rating})

    # Writing out of date value should not change anything
    put(entity, 2, VectorClock().update('c0', 0), port=dbp)
    rating, choices, clocks = get(entity, port=dbp)
    result({'type': 'float', 'expected': 1.0, 'got': rating})

    # Writing more recent value should overwrite
    put(entity, 3, VectorClock().update('c0', 5), port=dbp)
    rating, choices, clocks = get(entity, port=dbp)
    result({'type': 'float', 'expected': 3.0, 'got': rating})

def gossipTest(result):
    """ Run a gossip test, using whatever digest_length the test specified. """
    base = 'aardvark'
    firstEnt = base+'0'
    cv = VectorClock().update('c0', 1)
    put(firstEnt, 1, cv)
    dbid = getDBId(firstEnt)
    if dbid == -1:
        result({'type': 'KEY_NOT_SAVED'})
        return
    dbsub = [dbSubscriber(i) for i in (range(dbid, ndb)+range(0, dbid))[:ndb-1]]
    result({'type': 'int', 'entity': firstEnt, 'got': clients[dbid].hget(mkKey(firstEnt), 'rating'), 'expected': 1})

    # Overfill the digest and force a gossip push
    for i in range(1, digest_length+1):
        item = base + str(i)
        put(item, 1, cv, dbPort(dbid))

    # No gossip should have been pulled by the subscriber, as all the puts went to the publisher
    result({'type': 'bool', 'expected': False, 'got': str(clients[dbsub[0]].exists(base+'*'))})

    """
        Now put values directly into every other DB instance. This will force
        gossip into its neighbour in the chain. 
        Note that the entities stored are mildly out of spec---the DB instances in
        which the entities are being written are not necessarily those that would
        be the primary instance for those entities. But if the DB instances are
        working correctly, they should still obligingly save these values
        and force a gossip.
    """
    put('hello', 2, cv, dbPort(dbsub[0])) # Direct to DB
    result({'type': 'int', 'entity': firstEnt, 'got': clients[dbsub[0]].hget(mkKey(firstEnt), 'rating'), 'expected': 1})

    put('oliver', 3, cv, dbPort(dbsub[1])) # Direct to DB
    result({'type': 'int', 'entity': 'hello', 'got': clients[dbsub[1]].hget(mkKey('hello'), 'rating'), 'expected': 2})

    put('zoo', 4, cv, dbPort(dbsub[2])) # Direct to DB
    result({'type': 'int', 'entity': 'oliver', 'got': clients[dbsub[2]].hget(mkKey('oliver'), 'rating'), 'expected': 3})

    put(firstEnt, 5, VectorClock().update('c0', 2)) # Routed through LB
    result({'type': 'int', 'entity': firstEnt, 'got': clients[dbid].hget(mkKey(firstEnt), 'rating'), 'expected': 5})

@test()
def forceGossip(result):
    """ Send enough changes to force a gossip.
        This test uses the default digest length. Writes will be
        retained and only sent via gossip when the digest is full.
    """
    if ndb <= 1:
        result({'type': 'TEST_SKIPPED', 'reason': 'Only 1 database (no gossip)'})
    gossipTest(result)

@test()
def digestLength1(result):
    """ Test gossip with digest length of 1.
        This will force every write to immediately be sent to the queue.
    """
    configs = copy.deepcopy(db_configs)
    for config in configs:
        config['digest_length'] = 1
    dbTestConfigs['digestLength1'] = configs
    gossipTest(result)

@test()
def simpleOneDB(result):
    """ Test simple read/write/read sequence for multiple entities with N = 1.
        digest_length is set to 1 to force gossiping (which will do nothing,
        of course, because there is only one replica).
    """
    configs = [copy.deepcopy(db_configs[0])]
    configs[0]['ndb'] = 1
    configs[0]['digest_length'] = 1
    dbTestConfigs['simpleOneDB'] = configs

    # Read/Write/Read first entity
    r1  = 5
    time = 1
    entity1 = 'black-dragon-pearls-black-tea'
    r, ch, cl = get(entity1)
    testResult(result, r, 0.0, ch, [], cl, [])
    vc1 = VectorClock().update('c0', time)
    put(entity1, r1, vc1)
    r, ch, cl = get(entity1)
    testResult(result, r, r1, ch, [r1], cl, [vc1])

    # Intermingle Reads and Writes for second and third entities
    entity2 = 'cranberry-singapore-sling-rooibos-tea'
    r2 = 3
    vc2 = VectorClock().update('c1', 10)
    entity3 = 'grape-wulong-oolong-tea'
    r3 = 1
    vc3 = VectorClock().update('c2', 15)
    put(entity2, r2, vc2)
    put(entity3, r3, vc3)
    r, ch, cl = get(entity3)
    testResult(result, r, r3, ch, [r3], cl, [vc3])
    r, ch, cl = get(entity2)
    testResult(result, r, r2, ch, [r2], cl, [vc2])
    
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
    vc = VectorClock().update('c0', 1)
    put(first, 1, vc)

    dbId = getDBId(first)
    if dbId == -1:
        result({'type': 'KEY_NOT_SAVED'})
        return
    dbsub = dbSubscriber(dbId)

    for i in range(1,digest_length+1):
        item = base + str(i)
        put(item, 1, vc, dbPort(dbId))

    result({'type': 'bool', 'expected': False, 'got': clients[dbsub].exists(base+'*')})

    rating, _, _ = get('hello', port=dbPort(dbsub))
    result({'type': 'float', 'expected': 0.0, 'got': rating})

    rating, choices, clocks = get(first, port=dbPort(dbsub))
    result({'type': 'float', 'expected': 1.0, 'got': rating})
    result({'type': 'EXPECT_CHOICES', 'expected': [1.0], 'got': choices})
    result({'type': 'EXPECT_CLOCKS', 'expected': [vc.asDict()], 'got': [c.asDict() for c in clocks]})

@test()
def highVolume(result):
    """ Run (reasonably) high volume of updates. """
    queries = 1000
    sample = 20
    entSample = random.sample(entities, sample)
    dbVals = {} # Our record of what we've written to the DB
    for e in entSample:
        dbVals[e] = {'rating': 0.0, 'choices': [], 'clocks': []}

    for i in range(queries):
        entity = random.choice(entSample)
        if random.randrange(2) == 0:
            r, ch, cl = get(entity)
            '''
            # For debugging problems your code has passing this test,
            # comment out this section.
            # It will stop on the first incorrect rating. You
            # can then examine debugging output and the Redis DBs
            # (using the --leavedb option)
            if float(r) != float(dbVals[entity]['rating']):
                print entity, 'got', r, 'expected', dbVals[entity]
                for i in range(ndb):
                    print ('client', i, 'has', clients[i].hget(mkKey(entity), 'rating'),
                        clients[i].hget(mkKey(entity), 'choices'), clients[i].hget(mkKey(entity), 'clocks'))
                raise Exception('highVolume error')
            '''
            testResult(result, r, dbVals[entity]['rating'],
                               ch, dbVals[entity]['choices'], 
                               cl, dbVals[entity]['clocks'],
                       entity=entity)
        else:
            rate = random.randrange(5)
            # Specification is ambiguous about handling equal times, so
            # ensure new clock is distinct from value that should be stored in DB
            vc = VectorClock().update('c0', random.randrange(100))
            while dbVals[entity]['clocks']!=[] and dbVals[entity]['clocks'][0] == vc:
                vc = VectorClock().update('c0', random.randrange(100))

            if dbVals[entity]['clocks']==[] or dbVals[entity]['clocks'][0] < vc:
                dbVals[entity] = {'rating': rate, 'choices': [rate], 'clocks':[vc]}
            put(entity, rate, vc)


def doNoise(entities, client, time, maxRating, count):
    """ Slosh around the noise entities. 
        Half the accesses are reads, half writes.
    """
    reads = random.sample(range(count), int(count/2))
    for i in range(count):
        if i in reads:
            get(random.choice(entities))
        else:
            put(random.choice(entities), random.randrange(maxRating+1), VectorClock().update(client, time))
            time += 1
    return time

@test()
def convergence(result):
    """ Test that the system ultimately converges. """
    entSamp = random.sample(entities, 21)
    entMain = entSamp[0]
    entNoise = entSamp[1:]
    time = 20
    maxRating = 5
    client = 'c5'

    # Warm up the system with some noise entries
    time = doNoise(entNoise, client, time, maxRating, 30)

    # Write the main entity to primary
    rateMain = 1.0
    vcMain = VectorClock().update(client, time)
    time += 1
    put(entMain, 1, vcMain)

    # Slosh around the noise entities, forcing gossip
    time = doNoise(entNoise, client, time, maxRating, 100)

    # Check that every client has the value set for entMain
    for client in clients:
        testResult(result,
                   client.hget(mkKey(entMain), 'rating'), rateMain,
                   json.loads(client.hget(mkKey(entMain), 'choices')), [rateMain],
                   [VectorClock.fromDict(vcd) for vcd in json.loads(client.hget(mkKey(entMain), 'clocks'))], [vcMain],
                   entMain)

    # Check that a bunch of eventually consistent reads gives the right answer
    for i in range(20):
        r, ch, cl = get(entMain, ec=True)
        testResult(result,
                   r, rateMain,
                   ch, [rateMain],
                   cl, [vcMain],
                   entMain)
        

# Go through all the tests and run them
try:
    # Start our servers
    #startDBServers()

    for test in tests:
        if args.test == None or args.test == test.__name__:
            test()
finally:
    # Shut. down. everything.
    for lb_server in lb_servers: lb_server.terminate()
    stopDBServers()
    for qs_server in qs_servers: qs_server.terminate()
    if not args.leavedb:
        for p in rd_processes: p.terminate()

# Fin.
