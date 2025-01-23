## time delay to inspec docker image
import time
import logging
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

def main():
    """
    A function that runs for 10 minutes whilst logging its progress.
    """
    logger.info("Starting 10 minute monitoring period...")
    
    start_time = time.time()
    end_time = start_time + (5 * 60)  # 10 minutes in seconds
    
    while time.time() < end_time:
        elapsed_minutes = (time.time() - start_time) / 60
        remaining_minutes = 10 - elapsed_minutes
        logger.info(f"Time elapsed: {elapsed_minutes:.1f} minutes. Remaining: {remaining_minutes:.1f} minutes")
        time.sleep(60)  # Sleep for 1 minute between updates
    
    logger.info("10 minute monitoring period completed")