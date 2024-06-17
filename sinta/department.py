import logging
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from requests import get

import sinta
from util.config import get_config
from util.utils import cast, compact_list, format_output, listify, run_thread

log_format = "%(levelname)-8s %(asctime)s   %(message)s"
date_format = "%d/%m %H:%M:%S"
logging.basicConfig(format=log_format, datefmt=date_format, level=logging.DEBUG)


def department(department_ids, affiliation_id, output_format="dict", cache_path=None):
    """
    Fetches and returns department information for a given affiliation.

    Parameters:
    department_ids (list): List of department IDs to fetch information for.
    affiliation_id (str): The ID of the affiliation to fetch departments for.
    output_format (str, optional): The format to return the results in. Defaults to 'dict'.
    cache_path (str, optional): The path to the cache file. If None, a default path is used.

    Returns:
    dict/list: The department information, formatted according to the output_format parameter.
    """
    # create cache
    if cache_path is None:
        cache_path = Path.home() / ".cache/sinta/department/"
    cache_path.mkdir(exist_ok=True, parents=True)
    cache = cache_path / f"{affiliation_id}.json"
    if cache.exists():
        logging.debug(
            f"Cache found, loading departments information for affiliation: {affiliation_id}..."
        )
        df = pd.read_json(cache).T
    else:
        logging.debug(
            f"Cache not found, fetching departments information for affiliation: {affiliation_id}..."
        )
        df = pd.DataFrame.from_dict(fetch_all_department(affiliation_id)).set_index(
            "department_id_hash", drop=False
        )
        df.T.to_json(cache, indent=2)
        logging.debug(f"Saved department cache in: {cache}")

    hashed_department_ids = []
    hashed_univ_ids = []
    subset = df[df.department_id.isin(listify(department_ids))]
    assert len(subset) > 0, "No departments found for the given department IDs."
    hashed_department_ids = subset.department_id_hash.to_list()
    hashed_univ_ids = subset.univ_id_hash.unique()

    result = run_thread(
        worker,
        listify(hashed_department_ids),
        affiliation_id=affiliation_id,
        affiliation_code=hashed_univ_ids,
    )
    result = compact_list(result)

    return format_output(result, output_format)


def fetch_all_department(affiliation_id):
    """
    Fetches all department information for a given affiliation.

    Parameters:
    affiliation_id (str): The ID of the affiliation to fetch departments for.

    Returns:
    list: A list of dictionaries, each containing information about a department.
    """
    worker_result = []
    affiliation_code = sinta.affiliation(affiliation_id)["code"]
    domain = get_config()["domain"]
    ctr = 0
    page = 1
    while True:
        url = f"{domain}/affiliations/departments/{affiliation_id}/{affiliation_code}?page={page}"
        logging.debug(f"Fetching page {page}: {url}")
        html = get(url)
        soup = BeautifulSoup(html.content, "html.parser")

        # get affiliation info
        univ_info = soup.select_one(".univ-name")
        univ_name = univ_info.select_one("h3 a").text.strip()
        univ_url = univ_info.select_one("h3 a")["href"]
        univ_abbrev = univ_info.select_one(".affil-abbrev").text.strip()
        univ_location = univ_info.select_one(".affil-loc").text.strip()
        univ_codex = univ_info.select_one(".affil-code").text.strip().split(" ")

        affiliation = {
            "id": univ_codex[2],
            "code": univ_codex[6],
            "name": univ_name,
            "univ_abbrev": univ_abbrev,
            "url": univ_url,
            "location": univ_location,
        }

        # Find all department rows
        department_rows = soup.select(".content-list-no-filter .d-item")
        if not department_rows:
            break  # Exit loop if no more department rows are found

        # Iterate over each department row
        for row in department_rows:
            name_tag = row.select_one(".tbl-content-name a")
            code_tag = row.select_one(".tbl-content-meta-num")
            level_tag = row.select_one(".col-lg-1.tbl-content-meta.mb-2")
            if (
                name_tag
                and code_tag
                and ("department" in name_tag["href"])
                & ("Authors" not in name_tag.text)
            ):
                department_info = {
                    "department_id": code_tag.text.strip(),
                    "name": name_tag.text.strip(),
                    "level": level_tag.text.strip(),
                    "full_name": f"{name_tag.text.strip()} ({level_tag.text.strip()})",
                    "url": name_tag["href"].lower(),
                    "department_id_hash": name_tag["href"].lower().split("/")[-1],
                    "univ_id_hash": name_tag["href"].lower().split("/")[-2],
                }
                department_info["affiliation"] = affiliation
                worker_result.append(department_info)
                ctr += 1
        logging.debug(f"Found {ctr} departments")
        page += 1  # Move to the next page
    return worker_result


def worker(department_id, worker_result, **kwargs):
    """
    Worker function to fetch and process department information.

    Parameters:
    department_id (str): The ID of the department to fetch information for.
    worker_result (list): The list to append the fetched department information to.
    **kwargs: Additional keyword arguments. Should include 'affiliation_id' and 'affiliation_code'.

    Returns:
    None. The fetched department information is appended to worker_result.
    """
    affiliation_id = kwargs["affiliation_id"]
    affiliation_code = kwargs["affiliation_code"]
    domain = get_config()["domain"]
    url = f"{domain}/departments/profile/{affiliation_id}/{affiliation_code}/{department_id}"
    html = get(url)
    soup = BeautifulSoup(html.content, "html.parser")

    name = soup.select(".univ-name > h3")[0].text.strip()
    location = soup.select(".affil-loc")[0].text.strip()
    affiliation_soup = soup.select(".meta-profile > a")[0]
    affiliation = {
        "id": cast(affiliation_soup["href"].split("/")[-1]),
        "name": affiliation_soup.text.strip(),
        "url": affiliation_soup["href"],
    }

    result_data = {
        "id": department_id,
        "name": name,
        "location": location,
        "url": url,
        "affiliation": affiliation,
    }

    worker_result.append(result_data)
