import logging
import sys
import os

def get_logger(name: str) -> logging.Logger:
    """
    Set up a tool to record what the program is doing. 
    It will show messages on the screen and save them to a file.
    """
    logger = logging.getLogger(name)
    
    # Make sure we do not print the exact same message twice
    if not logger.hasHandlers():
        # Only record messages that are INFO level or higher (like warnings or errors)
        logger.setLevel(logging.INFO)
        
        # Set how the message looks: [Time] | [Type of message] | [Module Name]: The message
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Step 1: Show messages on the terminal screen
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Step 2: Save messages into a text file so we can read them later
        os.makedirs("logs", exist_ok=True)
        file_handler = logging.FileHandler("logs/pipeline.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    return logger