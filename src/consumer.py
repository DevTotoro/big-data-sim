from json import loads
from kafka import KafkaConsumer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_API_VERSION

if __name__ == '__main__':
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: loads(v.decode('utf-8'))
    )

    try:
        for message in consumer:
            print(message)
    except KeyboardInterrupt:
        print('Stopped by user. Shutting down...')

    consumer.close()
