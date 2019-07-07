import os

import redis
from rq import Worker, Queue, Connection

listen = ['high', 'default', 'low']

redis_url = os.getenv('REDIS_URL', 'redis://redistogo:56c98074b6abb5cbe4bb9b07346c63f6@pearlfish.redistogo.com:10069/')

conn = redis.from_url(redis_url)

print("worker***")

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        print("worker listen***")
        worker.work()