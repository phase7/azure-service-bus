import logging
from datetime import datetime
import typer

from auth import connection_string, topic_name
from abstractions import ServiceBusPublisher, ServiceBusClientFactory, TopicMessageSenderStrategy

logger = logging.getLogger("publisher")
logger.setLevel(logging.DEBUG)
# # stop azure.service bus logging to console
logging.getLogger('azure.servicebus').setLevel(logging.WARNING)


client = ServiceBusClientFactory.get_client(connection_string())
publisher = ServiceBusPublisher(connection_string=connection_string(),
                                name=topic_name(),
                                strategy=TopicMessageSenderStrategy)


def publish_from_cli(message: str):
    publisher.send_message(message_content=message)


if __name__ == "__main__":
    publisher.send_message(
        message_content=f"Hello, World! it's {datetime.now()}")
    typer.run(publish_from_cli)
