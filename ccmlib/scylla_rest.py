
import json
import requests

class ScyllaREST():
    def __init__(self, ip = '127.0.0.1', port = 10000):
        self.ip = ip
        self.port = port
        self.url = 'http://{}:{}'.format(ip, port)

    def post(self, item, params):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
    
        response = requests.post('{}/{}'.format(self.url, item), headers=headers, params=params)
        return response
    
    def get(self, item, params):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        
        response = requests.get('{}/{}'.format(self.url, item), headers=headers, params=params)
        return response
