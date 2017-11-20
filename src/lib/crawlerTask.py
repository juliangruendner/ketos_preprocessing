import threading
import time
from lib import mongodbConnection
from lib import crawler
from datetime import datetime

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
                next_job = mongodbConnection.get_db().crawlerJobs.find_one({"status": "queued"})
                
                if(next_job is None):
                    continue

                mongodbConnection.get_db().crawlerJobs.update({"_id": next_job["_id"]}, {"$set": {"status": "running", "start_time": str(datetime.now())}})

                for subject in next_job["patients"]:
                    crawler.crawlResourceForSubject(next_job["resource"], subject, next_job["_id"])
                    mongodbConnection.get_db().crawlerJobs.update({"_id": next_job["_id"]}, {"$push": {"finished": subject}})

                mongodbConnection.get_db().crawlerJobs.update({"_id": next_job["_id"]}, {"$set": {"status": "finished", "end_time": str(datetime.now())}})

                time.sleep(self.interval)
