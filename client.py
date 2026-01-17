import os
import time
import random
import requests

class Client:
    def __init__(self):
        self.client_id = self._load_client_id()
        self.process_url = self._load_process_url()

    def _load_client_id(self):
        client_id = os.getenv("CLIENT_ID")
        if client_id is None:
            raise RuntimeError("CLIENT_ID não definido no ambiente")
        return int(client_id)

    def _load_process_url(self):
        process_url = os.getenv("PROCESS_URL")
        if process_url is None:
            raise RuntimeError("PROCESS_URL não definido no ambiente")
        return process_url

    def generate_request_timestamp(self):
        return int(time.time() * 1000)

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

        return response

    def wait_for_committed(self, response):
        data = response.json()
        if data.get("status") != "COMMITTED":
            raise RuntimeError("Resposta inválida do Cluster Sync")

    def sleep_random(self):
        sleep_time = random.uniform(1, 5)
        time.sleep(sleep_time)

    def run_loop(self):
        for _ in range(30):
            response = self.send_write_request()
            self.wait_for_committed(response)
            self.sleep_random()