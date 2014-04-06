#  Storage node for Assignment 6, CMPT 474, Spring 2014

# Core libraries
import os
import sys
import time
import math
import json
import pdb

# Libraries that have to have been installed by pip
import redis
import requests
import mimeparse
from bottle import route, run, request, response, abort

# Local libraries
from queueservice import Queue
from vectorclock import VectorClock

base_DB_port = 3000

# These values are defaults for when you start this server from the command line
# They are overridden when you run it from test/run.py
config = { 'id': 0,
           'servers': [{ 'host': 'localhost', 'port': 6379 }],
           'hostport': base_DB_port,
           'qport': 6000,
           'ndb': 1,
           'digest-length': 1}

if (len(sys.argv) > 1):
    config = json.loads(sys.argv[1])

# Gossip globals
qport = config['qport']
queue = Queue(qport)
id = config['id']
digest_list = []

current_channel = 'db'+id
neighbour_channel = 'db'+((id+1)%ndb)
db_id_key = 'db_id'

# Connect to a single Redis instance
client = redis.StrictRedis(host=config['servers'][0]['host'], port=config['servers'][0]['port'], db=0)

# gets the average of tea-x.  key = '/rating/tea-x'
def average(key):
	teaHash = client.hgetall(key)
	sum = 0
	for clockJson, rating in teaHash.iteritems():
		sum = sum + rating
	if sum == 0: return None
	return sum/len(teaHash)	

# Figure out what to do given the clock vectors and then merge it
# 3 cases: new, incomparable, and stale
def coalesce(v1_cached, v2_input):

        # check to make sure it the arguments are VectorClock objects
        error_msg = 'Must be a VectorClock object'
        if not isinstance(v1_cached, VectorClock):
                print(error_msg)
                return abort(400)
        if not isinstance(v2_input, VectorClock):
                print(error_msg)
                return abort(400)

        # new input is most recent so lets return that one
        if v1_cached < v2_input:
                return [v2_input]

        # Incomparable data, let us return both
        if (v1_cached < v2_input) == False and (v1_cached > v2_input) == False:
                return [v1_cached, v2_input]

        # Well then...the new one is older so return the cached
        return [v1_cached]

# merge one pair of (clock, raiting)  
def merge_clock(rating, clock, key):
	
	# make sure the clock is a VectorClock object first
	if not isinstance(clock, VectorClock) and isinstance(clock, dict):
		clock = VectorClock.fromDict(clock)
	clockDict = clock.asDict()
	
	# lets get the hash from redis for this tea-x
	teaHash = client.hgetall(key)

	#flag so we know whether to just add the new rating or not
	isClientExist = False

	for clockJsonString, redisRating in teaHash.iteritems():
		redisClockDict = json.loads(clockJsonString)
		redisClock = VectorClock.fromDict(redisClockDict)
	
		# Check if the clock is inside the redis
		if clock >= redisClock or clock < redisClock:
			isClientExist = True # well, looks like we won't be creating a new one

			# returns either [redisclock], [clock], or [redisClock, clock]
			# which means stale, recent, incomparable respectively
			vcl = coalesce(redisClock, clock)
		
			# lets cache the comparisons
			redisClockIncluded = redisClock in vcl
			clockIncluded = clock in vcl

			# the incomparable case, include both!
			if redisClockIncluded and clockIncluded:
				# The redis (clock,rating) is already added so we just add
				# the new one to redis
				client.hset(key, json.dumps(clockDict), rating)
				
				# check if we can delete the old clock if a client in the  new clock 
				# is newer than an old client's clock time.
				for client, clockTime in redisClock: # iterate through each client
					if redisClockDict[client] < clockTime:
						client.hdel(key, json.dumps(redisClockDict))
		
			
			# the more recent case, replace the old one with the new one
			if not redisClockIncluded and clockIncluded:
				client.hdel(key, json.dumps(redisClockDict))
				client.hset(key, json.dumps(clockDict), rating)

	# Client never rated yet since we didn't find it in the hash so add it
	if isClientExist == False:
		client.hset(key, json.dumps(clockDict), rating)
	
	# still needs to return result as {rating, choices, clock}
	return None 

def merge_with_db(setrating, setclock, key):
    # fix this
    teaHash = merge_clock({setclock:setrating}, client.hgetall(key))

    db_instance = current_channel

    # Save the merged values into the Redis database.
    client.hmset(key, teaHash)

    # get rating choices clocks here
    digest_list.append(db_instance, key, rating, choices, clocks)

    if len(digest_list) >= config['digest-length']:
        for row in digest_list:
            queue.put(current_channel, row)
    else:
        return False
    return True

# checks the our channel for any messages then sync with it, also
# update the digest_list 
def sync_with_neighbour_queue():
    for queue_resp in queue.get(neighbour_channel):
        # According to Ted & Izaak, queue.get(neighbour_channel) automatically
        # dequeues the item from queue once called. Note that this issue has been,
        # raised multiple times, as noted by Izaak but he said to verify first.
        # If does not queue, I suggest implementing a mock queue.dequeue(channel)
        # method. 
        for primary_id, rating, choices, clocks in queue_resp.items()
            if current_channel != primary_id:
                merged_results = merge_clock({rating, choices, clocks}, {})
                client.hmset(key, merged_results) # this is broken
                merged_results[db_id_key] = primary_id
                digest_list.append(merged_results)
    return

# A user updating their rating of something which can be accessed as:
# curl -XPUT -H'Content-type: application/json' -d'{ "rating": 5, "choices": [3, 4], "clocks": [{ "c1" : 5, "c2" : 3 }] }' http://localhost:3000/rating/bob
# Response is a JSON object specifying the new average rating for the entity:
# { rating: 5 }
@route('/rating/<entity>', method='PUT')
def put_rating(entity):
    # Check to make sure JSON is ok
    mimetype = mimeparse.best_match(['application/json'], request.headers.get('Accept'))
    if not mimetype: return abort(406)

    # Check to make sure the data we're getting is JSON
    if request.headers.get('Content-Type') != 'application/json': return abort(415)

    response.headers.append('Content-Type', mimetype)

    # Parse the request
    data = json.load(request.body)
    setrating = data.get('rating')
    setclock = VectorClock.fromDict(data.get('clock'))

    key = '/rating/'+entity

    merge_with_db(setrating, setclock, key)
    sync_with_neighbour_queue()

    finalrating = 0.0
    # SAVE NEW VALUES

    # GOSSIP

    # Return rating
    return {
            "rating": finalrating
    }

# Get the aggregate rating of entity
# This can be accesed as:
#   curl -XGET http://localhost:3000/rating/bob
# Response is a JSON object specifying the mean rating, choice list, and
# clock list for entity:
#   { rating: 5, choices: [5], clocks: [{c1: 3, c4: 10}] }
# This function also causes a gossip merge
@route('/rating/<entity>', method='GET')
def get_rating(entity):
    key = '/rating/' + entity

    # YOUR CODE HERE
    # GOSSIP
    # GET THE VALUE FROM THE DATABASE
    # RETURN IT, REPLACING FOLLOWING
    sync_with_neighbour_queue()
    
    return {
        'rating': 0.0,
        'choices': [],
        'clocks': []
    }

# Delete the rating information for entity
# This can be accessed as:
#   curl -XDELETE http://localhost:3000/rating/bob
# Response is a JSON object showing the new rating for the entity (always null)
#   { rating: null }
@route('/rating/<entity>', method='DELETE')
def delete_rating(entity):
    # ALREADY DONE--YOU DON'T NEED TO ADD ANYTHING
    count = client.delete('/rating/'+entity)
    if count == 0: return abort(404)
    return { "rating": None }

# Fire the engines
if __name__ == '__main__':
    run(host='0.0.0.0', port=os.getenv('PORT', config['hostport']), quiet=True)
