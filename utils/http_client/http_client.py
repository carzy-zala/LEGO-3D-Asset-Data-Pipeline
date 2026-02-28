import requests
from pathlib import Path

class HttpClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def download_file(self, path: str, destination: str) -> bool:
        """
        Downloads a file and saves to destination path.
        Used for Rebrickable CSV and LDraw .dat downloads.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        Path(destination).parent.mkdir(parents=True, exist_ok=True)

        r = requests.get(url, stream=True, timeout=self.timeout)
        if r.status_code >= 400:
            raise requests.HTTPError(f"HTTP {r.status_code} for {r.url}", response=r)

        with open(destination, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True