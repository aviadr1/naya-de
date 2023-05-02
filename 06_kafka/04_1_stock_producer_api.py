'''
https://rapidapi.com/mpeng/api/stock-and-options-trading-data-provider/
'''

import json
import requests
from kafka import KafkaProducer
topic3 = 'stook'
brokers = ['cnt7-naya-cdh63:9092']

url = "https://stock-and-options-trading-data-provider.p.rapidapi.com/options/aapl"

# 
# if the example fails, most likely it is because the key has been over-used
# go to this URL to get your own "X-RapidAPI-Proxy-Secret" and "X-RapidAPI-Key" tokens
# 	https://rapidapi.com/mpeng/api/stock-and-options-trading-data-provider
#
headers = {
	"X-RapidAPI-Proxy-Secret": "a755b180-f5a9-11e9-9f69-7bf51e845926",
	"X-RapidAPI-Key": "ISQDktKE8On9ymlBNr7MPOeizRYiPgEY",
	"X-RapidAPI-Host": "stock-and-options-trading-data-provider.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)
response.raise_for_status()

try:
    # if this fails, run `pip install requests_to_curl`
	import requests_to_curl
	requests_to_curl.parse(response)
except ImportError:
	print()
	print('run pip install requests_to_curl to see the curl command')
	print()
    
row = response.json()
print(response.text[:200])
print('...')

producer = KafkaProducer(
	bootstrap_servers=brokers,
	client_id='producer',
	acks=1,
	compression_type=None,
	retries=3,
	reconnect_backoff_ms=50,
	reconnect_backoff_max_ms=1000,
	value_serializer=lambda v: json.dumps(row).encode('utf-8'))

producer = KafkaProducer(bootstrap_servers=brokers)
producer.send(topic=topic3, value=json.dumps(row).encode('utf-8'))
producer.flush()

