import os
import shutil
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from hdfs import InsecureClient

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

class Handler(FileSystemEventHandler):
    def __init__(self, hdfs_client):
        self.hdfs_client = hdfs_client

    def on_created(self, event):
        if event.is_directory:
            return None
        else:
            self.process(event)

    def process(self, event):
        print(f"Received created event - {event.src_path}")
        try:
            # Get current date
            current_day = datetime.now().strftime("%Y-%m-%d")
            hdfs_directory = f"/user/itversity/q-company_raw_layer/raw_sales_transactions_{current_day}"

            # Create HDFS directory if it does not exist
            if not self.hdfs_client.status(hdfs_directory, strict=False):
                self.hdfs_client.makedirs(hdfs_directory)
                print(f"Created HDFS directory: {hdfs_directory}")
            else:
                print(f"{hdfs_directory} already exists")

            # Upload file to HDFS
            file_name = os.path.basename(event.src_path)
            hdfs_path = f"{hdfs_directory}/{file_name}"
            with open(event.src_path, 'rb') as local_file:
                self.hdfs_client.write(hdfs_path, local_file, overwrite=True)
            print(f"File {event.src_path} uploaded to HDFS at {hdfs_path}")

            # Move file to retention area
            retention_area_path = f"/data/retention_area/{file_name}"
            shutil.move(event.src_path, retention_area_path)
            print(f"File {event.src_path} moved to {retention_area_path}")

            # Remove all files and directories in the staging area
            for root, dirs, files in os.walk("/data/staging_area", topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            print("Cleaned up staging area")

        except Exception as e:
            print(f"Failed to process {event.src_path}: {str(e)}")

if __name__ == '__main__':
    hdfs_client = InsecureClient('http://itvdelab:9870', user='itversity')
    w = Watcher(hdfs_client)
    w.run()
