import os
import time
import random
import requests

class Client: 
    def __init__(self):
        self.client_id = self._load_client_id()

    def _load_client_id(self):
        client_id = os.getenv("CLIENT_ID")

        if client_id is None:
            raise RuntimeError("Cliente_ID não definido no ambiente")
        
        return int(client_id)
    

    def generate_request_timestamp(self):
        timestamp_ms = int(time.time() * 1000)
        return (timestamp_ms,self.client_id)
    

    def send_write_request(self):
        payload = {
            "client_id": self.client_id,
            "timestamp_ms": self.generate_request_timestamp()
        }

        response = requests.post(
            self.process_url,
            json=payload,
            timeout=60
        )

        return response.json()
    
    def wait_for_commited(self,response):
        data = response.json()
        if data.get("status") != "COMMITED":
            raise RuntimeError("Resposta invalida do CLuster Sync")
        
    def sleep_random(self):
        sleep_time = random.uniform(1,5)
        time.sleep(sleep_time)

    def run_loop(self):
        for i in range(30):
            response = self.send_write_request()
            self.wait_for_commited(response)
            self.sleep_random()
