import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging():
    """Configures centralized logging for the application."""
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_file = os.path.join(log_directory, "processor.log")

    # Create a logger object
    logger = logging.getLogger("PDF_Processor")
    logger.setLevel(logging.INFO) # Set the minimum level of messages to capture

    # Prevent messages from being duplicated in the console if run directly
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a handler that writes log records to a file, with rotation
    # 10 MB per file, keeping the last 5 files.
    handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    
    # Create a formatter and set it for the handler
    # Format: Timestamp - Level - Message
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger

# Create a global logger instance that other modules can import
log = setup_logging()