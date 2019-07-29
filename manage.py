from flask_script import Manager
from webapp import APP
import os
import redis
from rq import Queue
from worker import conn
from aamain import startjobForYesterday

q = Queue(connection=conn)

manager = Manager(APP)

@manager.command
def queuestartJob(appname, packages):
    print("queuestartJob called")
    print(appname)
    print(packages)
    result = q.enqueue(startjobForYesterday, appname, packages)
    print("queuestartJob complete")

if __name__ == "__main__":
    manager.run()
