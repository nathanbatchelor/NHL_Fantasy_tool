import requests
import time


def safe_get(url: str, retries: int = 3, delay: float = 0.3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
