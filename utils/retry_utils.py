import requests
import time
from requests.exceptions import RequestException, HTTPError


def safe_get(url: str, retries: int = 5, delay: float = 1.0):
    """
    Fetches a URL with retries and exponential backoff for 429 errors.
    """
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()  # Raises HTTPError for 4xx/5xx
            return resp

        except HTTPError as e:
            # Check for 429 "Too Many Requests"
            if e.response.status_code == 429:
                # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                wait_time = delay * (2**attempt)
                print(
                    f"   ! API 429 Error for {url}. Retrying in {wait_time:.1f}s... (Attempt {attempt + 1}/{retries})"
                )
                time.sleep(wait_time)
            else:
                # For other HTTP errors (404, 500, etc.), raise immediately
                print(f"   ✗ HTTP Error {e.response.status_code} for {url}. Giving up.")
                raise

        except RequestException as e:
            # For connection errors, timeouts, etc.
            wait_time = delay * (2**attempt)
            print(
                f"   ! RequestException ({e}). Retrying in {wait_time:.1f}s... (Attempt {attempt + 1}/{retries})"
            )
            time.sleep(wait_time)

    # If all retries fail
    print(f"   ✗ All {retries} retry attempts failed for {url}.")
    raise Exception(f"Failed to fetch {url} after {retries} attempts")
