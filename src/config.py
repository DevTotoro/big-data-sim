from os import environ

KAFKA_BOOTSTRAP_SERVERS = environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = environ.get('KAFKA_TOPIC', 'vehicle_positions')
KAFKA_API_VERSION = environ.get('KAFKA_API_VERSION', '7.6.1')

SIMULATION_TIME = int(environ.get('SIMULATION_TIME', 3600))
SIMULATION_STOPPED_LINKS = environ.get('SIMULATION_STOPPED_LINKS', [
    'waiting_at_origin_node',
    'trip_end',
    'trip_aborted'
])

MESSAGE_INTERVAL = int(environ.get('MESSAGE_INTERVAL', 5))