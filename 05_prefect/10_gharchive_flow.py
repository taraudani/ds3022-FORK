import gzip
import json
import logging
from collections import Counter
from pathlib import Path
import requests
from prefect import flow, task


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"

# log to a local file next to this script
logger = logging.getLogger("gharchive")
logger.setLevel(logging.INFO)
_fh = logging.FileHandler(Path(__file__).with_name("gharchive.log"))
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_fh)


@task(retries=3)
def count_events_for_hour(hour):
    # stream one hourly file and return {event_type: count}
    counts = Counter()
    with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
        r.raise_for_status()  # a bad HTTP status raises an error, so Prefect retries
        # unzip the stream as it downloads; each line is one JSON event
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for line in gz:
                try:
                    counts[json.loads(line)["type"]] += 1
                except (json.JSONDecodeError, KeyError) as e:
                    # skip a broken line instead of failing the whole hour
                    logger.warning(f"hour {hour}: skipped bad line ({e})")
    logger.info(f"hour {hour}: {counts.total()} events")
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    # print those to screen and return them
    pass


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
