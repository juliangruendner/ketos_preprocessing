import threading
import time
from lib import mongodbConnection

class CrawlerTask(object):

    def __init__(self, app, interval=10):
        self.interval = interval
        self.app = app
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = False                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):
        with self.app.app_context():
            while True:
                next_job = mongodbConnection.get_db().crawlerJobs.find_one({"status": "queued"})
                print(next_job)
                time.sleep(self.interval)
