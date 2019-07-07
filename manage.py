from flask_script import Manager
from myapp import app
import os
import redis
from rq import Queue
from worker import conn
from utils import processaa

q = Queue(connection=conn)

manager = Manager(app)

@manager.command
def hello():
    result = q.enqueue(processaa, 'http://heroku.com')
    print(result)
    print("hello")


if __name__ == "__main__":
    manager.run()
