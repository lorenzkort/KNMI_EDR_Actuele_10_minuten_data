import logging
import os

def setup_logging(name):
    """Set up logging configuration for the given module name."""
    logger = logging.getLogger(name)
    
    # Set log level from environment variable or default to INFO
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    logger.setLevel(getattr(logging, log_level))
    
    # Create console handler if logger doesn't already have handlers
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
    
    return logger 