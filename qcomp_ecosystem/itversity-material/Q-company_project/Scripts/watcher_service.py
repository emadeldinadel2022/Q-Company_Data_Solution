import os
import shutil
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from hdfs import InsecureClient
import logging

class Watcher:
    DIRECTORY_TO_WATCH = "/data/staging_area"

    def __init__(self, hdfs_client):
        self.observer = Observer()
        self.hdfs_client = hdfs_client

    def run(self):
        event_handler = Handler(self.hdfs_client)
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer Stopped")

        self.observer.join()


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Handler(FileSystemEventHandler):
    def __init__(self, hdfs_client):
        self.hdfs_client = hdfs_client

    def on_created(self, event):
        if event.is_directory:
            return None
        else:
            self.process(event)

    def process(self, event):
        logging.info(f"Received created event - {event.src_path}")
        try:
            current_day = datetime.now().strftime("%Y-%m-%d")
            hdfs_directory = f"/user/itversity/q-company_raw_layer/raw_sales_transactions_{current_day}"

            if not self.hdfs_client.status(hdfs_directory, strict=False):
                self.hdfs_client.makedirs(hdfs_directory)
                logging.info(f"Created HDFS directory: {hdfs_directory}")
            else:
                logging.info(f"{hdfs_directory} already exists")

            file_name = os.path.basename(event.src_path)
            hdfs_path = f"{hdfs_directory}/{file_name}"

            self._wait_for_file_completion(event.src_path)

            self.hdfs_client.upload(hdfs_path, event.src_path, overwrite=True)
            
            if self._verify_upload(event.src_path, hdfs_path):
                logging.info(f"File {event.src_path} successfully uploaded to HDFS at {hdfs_path}")
            else:
                raise Exception("File verification failed")

            retention_area_path = f"/data/retention_area/{file_name}"
            shutil.move(event.src_path, retention_area_path)
            logging.info(f"File {event.src_path} moved to {retention_area_path}")

        except Exception as e:
            logging.error(f"Failed to process {event.src_path}: {str(e)}", exc_info=True)

    def _wait_for_file_completion(self, file_path, timeout=1000, check_interval=1):
        """Wait for the file to be completely written."""
        start_time = time.time()
        last_size = -1
        while time.time() - start_time < timeout:
            current_size = os.path.getsize(file_path)
            if current_size == last_size:
                return  # File size hasn't changed, assuming it's complete
            last_size = current_size
            time.sleep(check_interval)
        raise TimeoutError(f"Timeout waiting for file {file_path} to be completely written")

    def _verify_upload(self, local_path, hdfs_path):
        """Verify that the uploaded file size matches the local file size."""
        local_size = os.path.getsize(local_path)
        hdfs_size = self.hdfs_client.status(hdfs_path)['length']
        return local_size == hdfs_size

if __name__ == '__main__':
    hdfs_client = InsecureClient('http://itvdelab:9870', user='itversity')
    w = Watcher(hdfs_client)
    w.run()