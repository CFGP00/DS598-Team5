import logging
import requests
import schedule
import time
import json
from azure.eventhub import EventHubProducerClient, EventData

# Constants
FINNHUB_API_KEY = "" # deleted in github
EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = "realtimestock"

def fetch_and_send_stock_price():
    logging.basicConfig(level=logging.INFO)
    logging.info("Fetching AAPL stock price...")

    response = requests.get(
        "https://finnhub.io/api/v1/quote",
        params={"symbol": "AAPL", "token": FINNHUB_API_KEY}
    )

    if response.status_code == 200:
        data = response.json()
        current_price = data.get("c")

        if current_price is not None:
            payload = {
                "symbol": "AAPL",
                "price": current_price,
                "timestamp": int(time.time())
            }

            logging.info(f"Sending data: {payload}")

            producer = EventHubProducerClient.from_connection_string(
                conn_str=EVENT_HUB_CONN_STR,
                eventhub_name=EVENT_HUB_NAME
            )

            with producer:
                event_data = EventData(json.dumps(payload))
                producer.send_batch([event_data])
                logging.info("Data sent to Event Hub.")
        else:
            logging.warning("Price data not found in response.")
    else:
        logging.error(f"Failed to get stock data: {response.status_code}")

if __name__ == "__main__":
    schedule.every(1).minutes.do(fetch_and_send_stock_price)
    logging.info("Scheduler started. Running every 1 minute...")

    while True:
        schedule.run_pending()
        time.sleep(1)

