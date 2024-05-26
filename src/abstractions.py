import logging
from abc import ABC, abstractmethod
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceivedMessage
from typing import Callable
import threading

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class ServiceBusClientFactory:
    _instances = {}

    @staticmethod
    def get_client(connection_string: str) -> ServiceBusClient:
        if connection_string not in ServiceBusClientFactory._instances:
            ServiceBusClientFactory._instances[connection_string] = ServiceBusClient.from_connection_string(
                connection_string)
        return ServiceBusClientFactory._instances[connection_string]

# Strategy interfaces


class MessageSenderStrategy(ABC):
    @abstractmethod
    def send_message(self, client: ServiceBusClient, name: str, message_content: str) -> None:
        pass


class MessageReceiverStrategy(ABC):
    @abstractmethod
    def start_listening(self,
                        client: ServiceBusClient,
                        name: str,
                        message_handler: Callable[[ServiceBusReceivedMessage], None]) -> None:
        pass

# Queue strategy implementations


class QueueMessageSenderStrategy(MessageSenderStrategy):
    def send_message(self, client: ServiceBusClient, name: str, message_content: str) -> None:
        with client.get_queue_sender(queue_name=name) as sender:
            message = ServiceBusMessage(message_content)
            sender.send_messages(message)
            logging.info(f"Sent message to queue: {message_content}")


class QueueMessageReceiverStrategy(MessageReceiverStrategy):
    def start_listening(self,
                        client: ServiceBusClient,
                        name: str,
                        message_handler: Callable[[ServiceBusReceivedMessage], None]) -> None:
        def receive_messages() -> None:
            with client.get_queue_receiver(queue_name=name) as receiver:
                for msg in receiver:
                    message_handler(msg)
                    receiver.complete_message(msg)

        thread = threading.Thread(target=receive_messages)
        thread.daemon = True
        thread.start()
        logging.info(f"Started listening to queue: {name}")

# Topic strategy implementations


class TopicMessageSenderStrategy(MessageSenderStrategy):
    def send_message(self, client: ServiceBusClient, name: str, message_content: str) -> None:
        with client.get_topic_sender(topic_name=name) as sender:
            message = ServiceBusMessage(message_content)
            sender.send_messages(message)
            logging.info(f"Sent message to topic: {message_content}")


class TopicMessageReceiverStrategy(MessageReceiverStrategy):
    def start_listening(self,
                        client: ServiceBusClient,
                        name: str, subscription_name: str,
                        message_handler: Callable[[ServiceBusReceivedMessage], None]) -> None:
        def receive_messages() -> None:
            with client.get_subscription_receiver(topic_name=name,
                                                  subscription_name=subscription_name) as receiver:
                for msg in receiver:
                    message_handler(msg)
                    receiver.complete_message(msg)

        thread = threading.Thread(target=receive_messages)
        thread.daemon = True
        thread.start()
        logging.info(f"Started listening to topic subscription: {name}/{subscription_name}")


class ServiceBusPublisher:
    def __init__(self, connection_string: str, name: str, strategy: MessageSenderStrategy) -> None:
        self.client: ServiceBusClient = ServiceBusClientFactory.get_client(
            connection_string)
        self.name: str = name
        self.strategy: MessageSenderStrategy = strategy()

    def send_message(self, message_content: str) -> None:
        self.strategy.send_message(self.client, self.name, message_content)


class ServiceBusSubscriber:
    def __init__(self, connection_string: str, name: str, strategy: MessageReceiverStrategy) -> None:
        self.client: ServiceBusClient = ServiceBusClientFactory.get_client(
            connection_string)
        self.name: str = name
        self.strategy: MessageReceiverStrategy = strategy()

    def start_listening(self,
                        message_handler: Callable[[ServiceBusReceivedMessage], None],
                        subscription_name: str = None) -> None:
        if isinstance(self.strategy, TopicMessageReceiverStrategy):
            self.strategy.start_listening(
                self.client, self.name, subscription_name, message_handler)
        else:
            self.strategy.start_listening(
                self.client, self.name, message_handler)


def sample_message_handler(message: ServiceBusReceivedMessage) -> None:
    logging.info(f"Received message: {message}")

# Example usage (commented out for PCI compliance)
# connection_string = "Your Azure Service Bus connection string"
# queue_name = "Your queue name"
# topic_name = "Your topic name"
# subscription_name = "Your subscription name"

# For Queue
# queue_publisher = ServiceBusPublisher(connection_string, queue_name, QueueMessageSenderStrategy())
# queue_publisher.send_message("Hello, Queue!")

# queue_subscriber = ServiceBusSubscriber(connection_string, queue_name, QueueMessageReceiverStrategy())
# queue_subscriber.start_listening(sample_message_handler)

# For Topic
# topic_publisher = ServiceBusPublisher(connection_string, topic_name, TopicMessageSenderStrategy())
# topic_publisher.send_message("Hello, Topic!")

# topic_subscriber = ServiceBusSubscriber(connection_string, topic_name, TopicMessageReceiverStrategy())
# topic_subscriber.start_listening(sample_message_handler, subscription_name)
