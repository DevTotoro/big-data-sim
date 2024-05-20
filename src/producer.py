from time import sleep, time
from datetime import datetime
from json import dumps
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_API_VERSION, MESSAGE_INTERVAL, SIMULATION_STOPPED_LINKS
from simulation import run_simulation

if __name__ == '__main__':
    sim_data = run_simulation()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
        value_serializer=lambda v: dumps(v).encode('utf-8')
    )

    current_interval = 0
    start_time = time()

    try:
        max_interval = sim_data['t'].max()  # Maximum time interval in the simulation data

        while current_interval < max_interval:
            # Keep all data that is within the current interval and not stopped
            data_mask = (
                (sim_data['t'] <= current_interval) &
                (sim_data['t'] > current_interval - MESSAGE_INTERVAL) &
                (~sim_data['link'].isin(SIMULATION_STOPPED_LINKS))
            )
            filtered_data = sim_data[data_mask]

            for _, row in filtered_data.iterrows():
                event_time = datetime.fromtimestamp(start_time + row['t']).strftime("%d/%m/%Y %H:%M:%S")

                message = {
                    'name': row['name'],
                    'origin': row['orig'],
                    'destination': row['dest'],
                    'time': event_time,
                    'link': row['link'],
                    'position': row['x'],
                    'spacing': row['s'],
                    'speed': row['v']
                }

                producer.send(KAFKA_TOPIC, value=message)

            current_interval += MESSAGE_INTERVAL
            sleep(MESSAGE_INTERVAL)
    except KeyboardInterrupt:
        print('Stopped by user. Shutting down...')

    producer.flush()
    producer.close()
