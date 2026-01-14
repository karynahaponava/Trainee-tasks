import os
import time
import sys
from dotenv import load_dotenv
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler
from src.rules import FatalErrorTimeRule, BundleFatalErrorRule
from src.processor import LogProcessor
from src.logger import logger

load_dotenv()

INPUT_DIR = os.getenv("INPUT_DIR", "./data")


class NewFileHandler(FileSystemEventHandler):
    """Handles file system events.When a file is created, this class reacts immediately."""

    def __init__(self, processor):
        self.processor = processor

    def on_created(self, event):
        """Called when a file or directory is created."""
        if event.is_directory:
            return

        filename = event.src_path
        if not filename.endswith(".csv"):
            return

        time.sleep(0.5)

        logger.info(f"New file detected: {filename}")
        self._process_file(filename)

    def _process_file(self, filepath):
        try:
            self.processor.process(filepath)

            processed_path = filepath + ".processed"
            if os.path.exists(processed_path):
                os.remove(processed_path)

            os.rename(filepath, processed_path)
            logger.debug(f"Renamed to {processed_path}")

        except Exception as e:
            logger.error(f"Failed to process {filepath}: {e}")


def main():
    logger.info("Starting Log Analyzer Service (Event-Driven)...")

    os.makedirs(INPUT_DIR, exist_ok=True)

    active_rules = [FatalErrorTimeRule(), BundleFatalErrorRule()]
    processor = LogProcessor(active_rules)

    event_handler = NewFileHandler(processor)
    observer = Observer()
    observer.schedule(event_handler, path=INPUT_DIR, recursive=False)

    observer.start()
    logger.info(f"Monitoring started on: {INPUT_DIR}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Service stopping...")

    observer.join()


if __name__ == "__main__":
    main()
