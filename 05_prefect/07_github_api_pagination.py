"""
Work with Claude Code (CC) to complete this prefect flow.

The best way to do this is to open CC in the root dir of this
project, and your prompt can reference @05_prefect/07_github_api_pagination.py
to ask for a plan for how to complete the steps. the instructors
inline comments already add to the prompt - i.e. to paginate over that
API endpoint for a certain number of pages, and to store the output
in a single duckdb table. Ask CC to enumerate the steps you will need
to do this, and REQUIRE it to work with you on each task, one by one.

Read what CC generates/suggests with each step, and ask yourself if you
understand the python it has generated. IF YOU DO NOT UNDERSTAND IT WELL,
this is your chance to learn a bit more python! Ask CC to explain the specific
lines that are unclear to you, and have a back and forth with CC until you
get a sense for what is going on. You know about loops, and you know about
fetching remote web content, and so the growth here is understanding how
to cycle through a series of related links to build up all 1000 records.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import httpx
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

# copy all prefect logs (including print() via log_prints) into a local file
_fh = logging.FileHandler("github_pagination.log")
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger("prefect").addHandler(_fh)

# commit history for the git/git repository
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/repos/git/git/commits"
PER_PAGE = 100
PAGES = 10


# ------ only one task ------
# create a single prefect task that loops through 10 pages
# of git/git commits and stores each one into a duckdb table.
# add more tasks and more data if you like, and do more with it.
# if successful you should have 1000 records in your db.

@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    # work with claude to fetch 100 records for each page
    # and pull 10 pages total. This could be done in series,
    # or if you want to look at previous prefect .map examples
    # you could pull some/all pages concurrently.
    #
    # for help with github api pagination see
    # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2026-03-10
    #
    # httpx parses it into response.links. The "next" URL already includes 
    # per_page and page, so params are only needed on the first request. 
    # The loop ends when there is no "next" link (last page).

    # this task already comes with a name, retry settings, and anything
    # you print() is written to the log.
    # visit https://sds-prefect.pods.uvarc.io/v2/runs to review your flows

    # if you need to see a working example of this, see this file in this repo
    # 04_apis/05_pagination.py

    logger = get_run_logger()
    try:
        # follow the "next" Link header until it's gone or we hit PAGES
        url = URL
        params = {"per_page": PER_PAGE}   # only needed on the first request
        page = 1
        rows = []   # accumulates every commit across all pages

        while url and page <= PAGES:
            response = httpx.get(url, headers=HEADERS, params=params)
            response.raise_for_status()   # 4xx/5xx -> exception, which triggers prefect retries
            commits = response.json()     # list of up to 100 commit dicts
            print(f"page {page}: {len(commits)} commits")

            # flatten each nested commit into a tuple matching the table columns
            for c in commits:
                rows.append((
                    c["sha"],
                    c["commit"]["author"]["name"],
                    c["commit"]["author"]["date"],
                    c["commit"]["message"].splitlines()[0],      # first line only
                    (c.get("author") or {}).get("login"),        # GitHub user, may be null
                    c["html_url"],
                ))

            url = response.links.get("next", {}).get("url")   # None on the last page
            params = None                                     # next URL already has per_page & page
            page += 1

        # local duckdb file, created if missing; closed automatically when the block exits
        with duckdb.connect("actions.duckdb") as conn:
            # one table holding every commit; replaced on each run so reruns don't duplicate
            conn.execute("""
                CREATE OR REPLACE TABLE commits (
                    sha         VARCHAR PRIMARY KEY,
                    author_name VARCHAR,
                    author_date TIMESTAMPTZ,
                    message     VARCHAR,
                    gh_login    VARCHAR,
                    html_url    VARCHAR
                )
            """)
            conn.executemany("INSERT INTO commits VALUES (?, ?, ?, ?, ?, ?)", rows)

            count = conn.execute("SELECT count(*) FROM commits").fetchone()[0]
            print(f"stored {count} commits in actions.duckdb")
    except Exception:
        logger.exception("ingest failed")   # message + full traceback to UI and log file
        raise                               # re-raise so prefect marks the task failed and retries


# name the flow something you can identify as yourself
@flow(name="hav7tz-flow")
def main_flow():
    ingest_data()

# defining a flow doesn't run it -- call it when the file is executed directly
if __name__ == "__main__":
    main_flow()
