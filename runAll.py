import subprocess
import sys
import os
import time
from pathlib import Path

ROOT = Path(__file__).parent
VENV = ROOT / '.venv'
REQ = ROOT / 'requirements.txt'

def ensure_venv_and_reqs():
    if not VENV.exists():
        subprocess.check_call([sys.executable, "-m", "venv", str(VENV)])

    # Windows vs Linux/Mac paths
    if os.name == "nt":  # Windows
        pip = VENV / "Scripts" / "pip.exe"
        python_exec = VENV / "Scripts" / "python.exe"
    else:
        pip = VENV / "bin" / "pip"
        python_exec = VENV / "bin" / "python"

    subprocess.check_call([str(pip), "install", "-r", str(REQ)])
    return python_exec


def docker_compose_up():
    subprocess.check_call(["docker", "compose", "pull"])
    subprocess.check_call(["docker", "compose", "up", "-d"])


def wait_for_kafka(host="localhost", port=9092, timeout=120):
    import socket
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("Kafka reachable")
                return True
        except Exception:
            time.sleep(1)
    raise RuntimeError("Kafka not reachable")

def wait_for_kafka(host="localhost", port=9092, timeout=60):
    import socket, time

    print("Waiting for Kafka…")
    start = time.time()

    while time.time() - start < timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect((host, port))
            sock.close()
            print("Kafka is READY")
            return True
        except:
            print("Kafka not ready yet… retrying…")
            time.sleep(2)

    raise TimeoutError("Kafka did not start within timeout")

if __name__ == "__main__":
    python_exec = ensure_venv_and_reqs()
    docker_compose_up()

    print("Waiting for Kafka…")
    wait_for_kafka()

    print("Running KafkaProducer.py…")
    wait_for_kafka()
    subprocess.check_call([str(python_exec), str(ROOT / "KafkaProducer.py")])

