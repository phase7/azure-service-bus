from dotenv import load_dotenv
import os

load_dotenv()

def connection_string():
    return os.getenv('SB_CONN_STRING')

def topic_name():
    return os.getenv('SB_TOPIC_NAME')

def namespace_name():
    return os.getenv('SB_NAMESPACE')

def subscription_name():
    return os.getenv('SB_SUBSCRIPTION_NAME')