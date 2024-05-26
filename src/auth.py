from dotenv import load_dotenv
import os

load_dotenv()

def connection_string():
    return os.getenv('SB_CONN_STRING')