import logging
from datetime import datetime

from auth import connection_string, topic_name, subscription_name
from abstractions import ServiceBusSubscriber, ServiceBusClientFactory, TopicMessageReceiverStrategy, sample_message_handler

logger = logging.getLogger("subscriber")
logger.setLevel(logging.DEBUG)
# # stop azure.service bus logging to console
logging.getLogger('azure.servicebus').setLevel(logging.WARNING)

client = ServiceBusClientFactory.get_client(connection_string())
subscriber = ServiceBusSubscriber(connection_string=connection_string(),
                                  name=topic_name(),
                                  strategy=TopicMessageReceiverStrategy)

if __name__ == "__main__":

    subscriber.start_listening(
        message_handler=sample_message_handler, subscription_name=subscription_name())
    try:
        while True:
            pass
    except KeyboardInterrupt:
        logging.info("Program terminated.")
