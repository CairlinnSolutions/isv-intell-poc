import os

import redis
from rq import Worker, Queue, Connection

listen = ['high', 'default', 'low']

redis_url = os.getenv('REDIS_URL', 'redis://h:p21a9381a75f7352fd56c9c68ed42a944ceda9bc73c0d9e71d6be56aa3911f312@ec2-3-218-78-240.compute-1.amazonaws.com:16839')

conn = redis.from_url(redis_url)

print("worker***")

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        print("worker listen***")
        worker.work()