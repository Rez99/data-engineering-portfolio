from pathlib import Path
import requests

URL = "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"

# Store dataset inside the container so it disappears when
# the container is removed.
DATA_DIR = Path("/data")
DATA_FILE = DATA_DIR / "2019-Oct.csv.gz"


def download_if_missing():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if DATA_FILE.exists():
        print(f"File already exists: {DATA_FILE}")
        return

    print("Downloading dataset...")

    response = requests.get(URL, stream=True)
    response.raise_for_status()

    total_bytes = int(response.headers.get("content-length", 0))
    downloaded_bytes = 0
    last_pct = -1

    with open(DATA_FILE, "wb") as f:
        for chunk in response.iter_content(chunk_size=2048 * 2048):
            if not chunk:
                continue

            f.write(chunk)
            downloaded_bytes += len(chunk)

            if total_bytes:
                pct = int(downloaded_bytes / total_bytes * 100)

                if pct > last_pct:
                    print(f"\rDownloaded {pct}%", end="", flush=True)
                    last_pct = pct

    print("\nDownload complete.")
    print(f"Saved to: {DATA_FILE}")


if __name__ == "__main__":
    download_if_missing()