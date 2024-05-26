# azure-service-bus

## setup

1. check the .env.example file and create a .env file with the same content. You will ned an azure account to get all connection strings.
2. run `poetry install`
3. run `poetry shell` to activate the virtual environment

## run subscriber

1. run `python src/subscriber.py`. It will run until you `KeyboardInterrupt` it.

## run publisher

1. run `python src/publisher.py <your message>`. It will send a message to the topic and exit.
