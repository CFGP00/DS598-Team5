import logging
import os
import requests
import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData

# Constants
FINNHUB_API_KEY = "" # api key
EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = "realtimestock"

def main(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("Timer is past due.")

    logging.info("Fetching AAPL stock price...")

    response = requests.get(
        "https://finnhub.io/api/v1/quote",
        params={"symbol": "AAPL", "token": FINNHUB_API_KEY}
    )

    if response.status_code == 200:
        data = response.json()
        current_price = data.get("c")
        logging.info(f"AAPL price: {current_price}")

        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONN_STR,
            eventhub_name=EVENT_HUB_NAME
        )

        with producer:
            event_data = EventData(f"AAPL: {current_price}")
            producer.send_batch([event_data])
            logging.info("Data sent to Event Hub.")
    else:
        logging.error(f"Failed to get stock data: {response.status_code}")
