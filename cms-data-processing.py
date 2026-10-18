import csv
import io
import json
import logging
import os
import re
import tempfile
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import Request, urlopen


CMS_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)

OUTPUT_DIR = Path(r"C:\\Users\\akuma\\OneDrive\\Desktop\\Python\\Files")
CMS_FILE = OUTPUT_DIR / "CMS-Data.json"
STATE_FILE = OUTPUT_DIR / "state.json"

MAX_WORKERS = 5
TIMEOUT_SECONDS = 120
USER_AGENT = "cms-hospitals-downloader"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

print(f"The output directory path - {OUTPUT_DIR}")


def download_json(url, output_file):
    """Fetch JSON from a URL, check the HTTP response, and save it.
    """
    request = Request(url, headers={"User-Agent": USER_AGENT})

    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        if not 200 <= response.status < 300:
            raise RuntimeError(
                f"Request failed with HTTP status {response.status}"
            )

        data = json.load(response)

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    return data

def load_state():
    """Read the previous run's tracking information, tracking information 
       will be empty for first run.
    """
    if not STATE_FILE.exists():
        return {}

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))

    if not isinstance(state, dict):
        raise ValueError(f"{STATE_FILE} must contain a JSON object")

    return state

def save_state(state):
    """Save tracking information safely using a temporary file.
       Need to keep tracking so that incremental load can be done.
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    temporary_file = STATE_FILE.with_suffix(".tmp")
    temporary_file.write_text(
        json.dumps(state, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary_file, STATE_FILE)

def load_hospital_csvs(datasets):
    """ This will read the json data and pull the data for hospitals.
        Save the URL of the required CSVs and metadata info into a list.
    """
    csv_files = []

    for dataset in datasets:
        if "Hospitals" not in (dataset.get("theme") or []):
            continue

        for number, distribution in enumerate(dataset.get("distribution") or []):
            url = distribution.get("downloadURL", "")

            if url:
                csv_files.append({
                    "dataset_id": dataset.get("identifier"),
                    "modified": dataset.get("modified"),
                    "url": url,
                    "distribution_number": number,
                })

    return csv_files

def task_key(task):
    """ Create unique file name for tracking.
    """
    return f"{task['dataset_id']}_{task['distribution_number']}"

def csv_output_path(task):
    filename = f"{task['dataset_id']}_{task['distribution_number']}.csv"
    return OUTPUT_DIR / filename

def to_snake_case(header):
    """ Need to convert CSV header name in snake case and remove spaces and
        other unnecessary characters.
    """
    text = unicodedata.normalize("NFKD", str(header))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    return text.strip("_").lower()


def make_unique_headers(headers):
    """ Need to create uniqure columns name if column name is same then
        add count in the column name.
    """
    result = []
    counts = {}

    for number, header in enumerate(headers, start=1):
        name = to_snake_case(header) or f"column_{number}"
        counts[name] = counts.get(name, 0) + 1

        if counts[name] > 1:
            name = f"{name}_{counts[name]}"

        result.append(name)

    return result

def download_csv(task):
    """Download one CSV and convert its column headers to snake_case."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = csv_output_path(task)
    temporary_path = None

    request = Request(task["url"], headers={"User-Agent": USER_AGENT})

    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            if not 200 <= response.status < 300:
                raise RuntimeError(
                    f"CSV request failed with HTTP status {response.status}"
                )

            text_stream = io.TextIOWrapper(
                response,
                encoding="utf-8-sig",
                newline="",
            )
            reader = csv.reader(text_stream)

            headers = next(reader, None)
            if headers is None:
                raise ValueError("The downloaded CSV is empty")

            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                suffix=".csv",
                dir=OUTPUT_DIR,
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                writer = csv.writer(temporary_file)
                writer.writerow(make_unique_headers(headers))
                writer.writerows(reader)

        os.replace(temporary_path, output_file)
        temporary_path = None
        return str(output_file)

    finally:
        if temporary_path and temporary_path.exists():
            temporary_path.unlink()

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Fetch and save the metastore JSON.
    logger.info("Downloading CMS metastore JSON")
    datasets = download_json(CMS_URL, CMS_FILE)

    if not isinstance(datasets, list):
        raise ValueError("Expected the metastore response to be a JSON list")

    # Step 2: Load information from the previous run.
    state = load_state()

    # Step 3: Find CSV files belonging to the Hospitals theme.
    tasks = load_hospital_csvs(datasets)

    # Step 4: Select files that are new, changed, or missing locally.
    pending_tasks = []

    for task in tasks:
        previous = state.get(task_key(task), {})
        output_file = csv_output_path(task)

        changed = (
            previous.get("modified") != task["modified"]
            or previous.get("url") != task["url"]
            or not output_file.exists()
        )

        if changed:
            pending_tasks.append(task)

    # Step 5: Download pending CSVs in parallel.
    failures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_csv, task): task
            for task in pending_tasks
        }

        for future in as_completed(futures):
            task = futures[future]

            try:
                output_file = future.result()

                # Update tracking information only after a successful download.
                state[task_key(task)] = {
                    "modified": task["modified"],
                    "url": task["url"],
                    "output_file": output_file,
                }
                save_state(state)

                logger.info(f"Saved {output_file}")

            except Exception as error:
                key = task_key(task)
                failures.append(task_key(task))
                logger.exception(f"Failed to download {key}: {error}")

    if failures:
        logger.error(f"{len(failures)} CSV(s) failed.")
        return 1

    logger.info("All changed Hospital CSVs are downloaded.")
    return 0


if __name__ == "__main__":
    main()