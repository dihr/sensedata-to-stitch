import json
import os

import requests


class SensedataAPI:
    def __init__(self):
        self.base_url = 'https://api.sensedata.io'
        self.token = os.getenv('SENSEDATA_TOKEN')

    def get_entity_data(self, entity_name: str, page: int) -> json:
        endpoint = f"{self.base_url}/v2/{entity_name}?page={page}&limit=500"
        headers = {
            'Authorization': self.token,
            'Content-Type': 'application/json',
        }
        response = requests.get(url=endpoint, headers=headers)
        response.raise_for_status()
        return response.json()
