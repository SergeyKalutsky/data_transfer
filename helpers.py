import os
import json
from datetime import datetime


def calculate_age(born: datetime) -> datetime:
    today = datetime.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def save_map(id_map: dict, identity: str):
    if not os.path.exists('map.json'):
        data = {}
    else:
        with open('map.json', 'r') as f:
            data = json.load(f)
    data[identity] = id_map
    with open('map.json', 'w') as f:
        json.dump(data, f)


def load_map(identity: str):
    with open('map.json', 'r') as f:
        data = json.load(f)[identity]
        data = {int(k): v for k, v in data.items()}
        return data