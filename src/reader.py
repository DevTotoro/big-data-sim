from pymongo import MongoClient
from pymongo.collection import Collection, Mapping, Any

from config import MONGO_URI, MONGO_DB_NAME, RAW_VEHICLE_DATA_COLLECTION, PROCESSED_VEHICLE_DATA_COLLECTION


def find_link_with_lowest_vehicle_count(data: Collection[Mapping[str, Any]], start_time: str, end_time: str):
    query_result = data.aggregate([
        {
            '$match': {
                'time': {
                    '$gte': start_time,
                    '$lte': end_time
                }
            }
        },
        {
            '$sort': {
                'vcount': 1
            }
        },
        {
            '$limit': 1
        },
        {
            '$project': {
                'link': 1,
                'vcount': 1,
                'vspeed': 1,
                'time': 1,
                '_id': 0
            }
        }
    ])

    return next(query_result)


def find_link_with_highest_avg_speed(data: Collection[Mapping[str, Any]], start_time: str, end_time: str):
    query_result = data.aggregate([
        {
            '$match': {
                'time': {
                    '$gte': start_time,
                    '$lte': end_time
                }
            }
        },
        {
            '$sort': {
                'vspeed': -1
            }
        },
        {
            '$limit': 1
        },
        {
            '$project': {
                'link': 1,
                'vcount': 1,
                'vspeed': 1,
                'time': 1,
                '_id': 0
            }
        }
    ])

    return next(query_result)


def find_longest_trip(data: Collection[Mapping[str, Any]], start_time: str, end_time: str):
    query_result = data.aggregate([
        {
            '$match': {
                'time': {
                    '$gte': start_time,
                    '$lte': end_time
                }
            }
        },
        {
            '$sort': {
                'position': -1
            }
        },
        {
            '$limit': 1
        },
        {
            '$project': {
                'destination': 1,
                'origin': 1,
                'link': 1,
                'name': 1,
                'position': 1,
                'spacing': 1,
                'speed': 1,
                'time': 1,
                '_id': 0
            }
        }
    ])

    return next(query_result)


if __name__ == '__main__':
    client = MongoClient(MONGO_URI)

    raw_data_collection = client[MONGO_DB_NAME][RAW_VEHICLE_DATA_COLLECTION]
    processed_data_collection = client[MONGO_DB_NAME][PROCESSED_VEHICLE_DATA_COLLECTION]

    link_result = find_link_with_lowest_vehicle_count(
        processed_data_collection,
        '21/05/2024 21:34:07',
        '21/05/2024 21:35:46'
    )
    speed_result = find_link_with_highest_avg_speed(
        processed_data_collection,
        '21/05/2024 21:30:00',
        '21/05/2024 21:35:46'
    )
    trip_result = find_longest_trip(
        raw_data_collection,
        '21/05/2024 21:30:00',
        '21/05/2024 21:35:46'
    )

    print('Link with least vehicles:', link_result)
    print('Link with highest average speed:', speed_result)
    print('Longest trip:', trip_result)
