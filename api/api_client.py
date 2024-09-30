import requests

class APIClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key

    def get_data(self, endpoint, params=None):
        headers = {'Authorization': f'Bearer {self.api_key}'}
        response = requests.get(f"{self.base_url}/{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        return response.json()