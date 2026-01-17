from flask import Flask, request, jsonify
import time
import random
class ClusterSync:
    def __init__(self, process_id: int, port: int):
        self.process_id = process_id
        self.port = port

        self.app = Flask(__name__)
        self._setup_routes()

    def sleep_random(self):
        sleep_time = random.uniform(1, 5)
        time.sleep(sleep_time)

    def _setup_routes(self):
        @self.app.route("/write", methods=["POST"])
        def write():
            data = request.get_json()
            if data is None:
                return jsonify({"error": "invalid request"}), 400
            
            self.sleep_random()
            ## Aqui tem q começar o token Ring 

            # Resposta mínima exigida
            return jsonify({"status": "COMMITTED"}), 200
        
    
    def start(self):
        self.app.run(
            host="0.0.0.0",
            port=self.port,
            debug=False
        )


if __name__ == "__main__":
    cluster = ClusterSync(process_id=0, port=5000)
    cluster.start()
