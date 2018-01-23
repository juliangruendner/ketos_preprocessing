import threading
import time
from lib import mongodbConnection
from lib import crawler
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

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
                    time.sleep(self.interval)
                    continue

                logger.info("executing new job")
                mongodbConnection.get_db().crawlerJobs.update({"_id": next_job["_id"]}, {"$set": {"status": "running", "start_time": str(datetime.now())}})

                for subject in next_job["patient_ids"]:
                    if next_job["resource"] is not None and next_job["resource"] is not "Observation":
                        crawler.crawlResourceForSubject(next_job["resource"], subject, next_job["_id"], next_job["search_params"])

                    else:
                        for feature in next_job["feature_set"]:
                            crawler.crawlResourceForSubject("Observation", subject, next_job["_id"], {feature["key"]: feature["value"]})

                    mongodbConnection.get_db().crawlerJobs.update({"_id": next_job["_id"]}, {"$push": {"finished": subject}})

                mongodbConnection.get_db().crawlerJobs.update({"_id": next_job["_id"]}, {"$set": {"status": "finished", "end_time": str(datetime.now())}})