from flask_script import Manager
from myapp import app
import os
import redis
from rq import Queue
from worker import conn
from aamain import processaa

q = Queue(connection=conn)

manager = Manager(app)

@manager.command
def queueprocessaa():
    print("queueprocessaa called")
    result = q.enqueue(processaa, 1)
    print("queueprocessaa complete")

if __name__ == "__main__":
    manager.run()
