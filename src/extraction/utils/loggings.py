import logging


def get_default_logger() -> logging.Logger:
    """Set up the default logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Define a logging setup to be used throughout the module
    logger = logging.getLogger("extraction-logger")
    logger.setLevel(logging.INFO)

    # Avoid adding handlers multiple times
    if not logger.hasHandlers():
        console_handler = logging.StreamHandler()  # Log to console
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(console_handler)

    # Test the logger
    logger.info("Logger has been configured successfully.")

    return logger


def set_logger(_logger: logging.Logger) -> None:
    """Set the logger to be used throughout the module."""
    global logger
    logger = _logger


logger = get_default_logger()
