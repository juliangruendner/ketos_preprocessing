import threading
import time
from lib import mongodbConnection

class CrawlerTask(object):

    def __init__(self, app, interval=10):
        self.interval = interval
        self.app = app
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = False                            
        thread.start()                                  

    def run(self):
        with self.app.app_context():
            while True:
                #next_job = mongodbConnection.get_db().crawlerJobs.find_one({"status": "queued"})
                print("peter")
                #mongodbConnection.get_db().crawlerJobs.update(next_job, {"status": "running"})

                time.sleep(self.interval)
