import logging

from auth import connection_string

logger = logging.getLogger("publisher")
# stop azure.service bus logging to console
logging.getLogger('azure.servicebus').setLevel(logging.WARNING)

logger.setLevel(logging.DEBUG)

# print("Publisher started", connection_string())
logger.info("Connection string: %s", connection_string())

