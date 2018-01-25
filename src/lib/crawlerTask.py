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
                crawler.executeCrawlerJob(next_job)