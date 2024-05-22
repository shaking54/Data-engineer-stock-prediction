from kafka import KafkaProducer
import logging
import requests
import json
import time
producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'Bitcoin'
topic_name = 'coin'

headers = {
 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
 'Content-Type': 'application/json'
}

class Listener():

    def on_data(self, data):
        logging.info(data)
        producer.send(topic=topic_name, value=json.dumps(data).encode('utf-8'))

    def on_error(self, status_code):
        if status_code == 420:
            return False
        
    def streaming(self):
        
        while True:
            url = 'https://query2.finance.yahoo.com/v8/finance/chart/btc-usd'
            response = requests.get(url, headers=headers)
            self.on_error(response.status_code)
            data = json.loads(response.text)
            price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            self.on_data(price)
            time.sleep(2)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    listener = Listener()
    listener.streaming()