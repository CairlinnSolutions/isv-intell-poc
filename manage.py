from flask_script import Manager
from webapp import app
import os
import redis
from rq import Queue
from worker import conn
from aamain import startjobForYesterday

q = Queue(connection=conn)

manager = Manager(app)

@manager.command
def queuestartJob(appname, packages):
    print("queuestartJob called")
    result = q.enqueue(startjobForYesterday, appname, packages)
    print("queuestartJob complete")

if __name__ == "__main__":
    manager.run()
