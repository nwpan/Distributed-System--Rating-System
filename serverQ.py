#  Q service for Assignment 6, CMPT 474, Spring 2014

# Core libraries
import os
import sys
import json

# Libraries that have to have been installed by pip
from bottle import route, run, request, response, abort

config = {'id':0, 'port': 6000, 'nq':1, 'ndb': 1 }
if (len(sys.argv) > 1):
    config = json.loads(sys.argv[1])

port = config['port']
ndb = config['ndb']

queue = {} # Dictionary of queue channels

# Push to the queue
# This can be accessed using;
#   curl -XPUT -H'Content-type: application/json' -d<message> http://localhost:6000/q/<channel>
# Response is a JSON object specifying the number of items in the channel:
#   { channel: name, length: 5 }
@route('/q/<channel>', method='PUT')
def put_item(channel):
    global queue
    # Check to make sure the data we're getting is JSON
    if request.headers.get('Content-Type') != 'application/json': return abort(415)

    if channel not in queue:
        queue[channel] = []
    queue[channel].append(request.body)

    response.headers.append('Content-Type', 'application/json')
    # Return the number of messages in the queue
    return {
        "channel": channel,
        "length": len(queue[channel])
    }


# Get the next item in a channel, or an empty dictionary if channel empty
# This can be accesed using:
#   curl -XGET http://localhost:6000/q/<channel>
# Response is a JSON object containing the next item
#   { rating: 5, choices: [5], clocks: [{c1: 3, c4: 10}] }
# or (for an empty channel)
#   {  }
@route('/q/<channel>', method='GET')
def get_item(channel):
    if channel in queue and len(queue[channel]) > 0:
        item, queue[channel] = queue[channel][0], queue[channel][1:]
        return item
    else:
        return {}

# Clear all the items currently in-flight in the queue.
# Use for debugging and testing.
# This can be accessed using:
#  curl -XDELETE http://localhost:6000/clear
# Response is a JSON object giving the  names
# of all channels and the number of in-flight items 
# in each of them before the clear
#  { 'db0': 2, 'db5': 12}
@route('/clear', method='DELETE')
def clear_queue():
    global queue
    chans = {}
    for key in queue:
        chans[key] = len(queue[key])
    queue = {}
    return chans


# Fire the engines
if __name__ == '__main__':
    run(host='0.0.0.0', port=os.getenv('PORT', port), quiet=True)
