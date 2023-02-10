import os
import requests
import argparse
import json
import time
import logging

import tqdm

from common import *


MAX_EXPORT = 200000
CHUNK_SIZE = 65536

class INaturalistExporter:
    def __init__(self, username, password):
        self.session = requests.Session()
        self.export_id = None
        self.logger = logging.getLogger("INaturalistExporter")
        
        # if username or password is None, prompt for credentials
        if username is None or password is None:
            username = input("Email: ")
            password = input("Password: ")
        
        self.login(username, password)


    def login(self, username, password):
        # get login page
        r = self.session.get("https://www.inaturalist.org/login")
        r.raise_for_status()
        
        # login
        data = {
            "utf8": "✓",
            "authenticity_token": self._parse_csrf(r),
            "user[email]": username,
            "user[password]": password,
            "user[remember_me]": "0",
        }
        
        r = self.session.post("https://www.inaturalist.org/session", data=data, allow_redirects=False)
        r.raise_for_status()
        
        if r.status_code != 302:
            self.logger.error(f"Login failed with status code {r.status_code}")
            raise ValueError("Login failed")
        else:
            self.logger.info("Login successful")
    
    
    def export(self, taxon_ids : list | int, export_dir : str = EXPORT_DIR):
        # convert to list if necessary
        if isinstance(taxon_ids, int):
            taxon_ids = [taxon_ids]
        
        # count results for the query
        query, query_encoded = self._get_query(taxon_ids)
        total_results = self._count_results(query)
    
        if total_results == False:
            raise ValueError("Failed to count results")
        if total_results == 0:
            raise ValueError("No results found")        
        if total_results > MAX_EXPORT:
            raise ValueError(f"Too many results ({total_results} > {MAX_EXPORT})")
        
        self.logger.debug(f"Found {total_results} results")

        # navigate to export page for the first time
        r = self.session.get("https://www.inaturalist.org/observations/export")
        r.raise_for_status()
    
        # build form
        form = self._build_form(query_encoded)
        
        # create headers
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://www.inaturalist.org/observations/export",
            "X-CSRF-Token": self._parse_csrf(r),
            "X-Requested-With": "XMLHttpRequest"
        }

        # post form
        r = self.session.post("https://www.inaturalist.org/flow_tasks", data=form, headers=headers)

        if r.status_code == 422:
            self.logger.error("Export failed with status code 422 (Unprocessable Entity)")
            raise ValueError(r.json()["error"])
        
        self.logger.info(f"Export request sent for ids {taxon_ids} with {total_results} results. Waiting for export to complete...")
        
        # get export ID  
        export_id = r.json()["id"]

        # wait for export to complete
        completed = False
        while not completed:
            r = self.session.get(f"https://www.inaturalist.org/flow_tasks/{export_id}/run.json")
            r.raise_for_status()

            data = r.json()
            
            if len(data["outputs"]) > 0:
                completed = True
            else:
                time.sleep(2)
    
        self.logger.info("Export complete")    

        # download export
        output = data["outputs"][0]
        filename = output["file_file_name"]
        url = f"https://www.inaturalist.org/attachments/flow_task_outputs/{output['id']}/{filename}"

        r = self.session.get(url, stream=True)
        total_size = int(r.headers.get("content-length", 0))
        
        download_path = os.path.join(export_dir, filename)
        
        with open(filename, "wb") as f:
            with tqdm.tqdm(total=total_size, unit="iB", unit_scale=True) as pbar:
                for chunk in r.iter_content(chunk_size=65535):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        self.logger.info(f"Export saved to {filename}")
        
    
    def _parse_csrf(self, response):
        return response.text.split('name="csrf-token" content="')[1].split('"')[0]

    def _get_query(self, taxon_ids : list):
        query_dict = {
            "has": ["photos"],
            "quality_grade": "any",
            "identifications": "any",
            "taxon_ids": taxon_ids,
        }

        query_encoded = "has[]=photos&quality_grade=any&identifications=any&taxon_ids[]=" + ",".join([str(x) for x in taxon_ids])

        return query_dict, query_encoded


    def _count_results(self, query : dict):
        r = requests.get("https://api.inaturalist.org/v1/observations", params=query)
        
        if r.status_code != 200:
            return False
        
        data = r.json()
        return data["total_results"]
    
    def _build_form(self, query : str):
        with open("form.json", 'r') as f:
            form = json.load(f)
                        
        form["observations_export_flow_task[inputs_attributes][0][extra][query]"] = query
        return form
        
        
def get_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("taxon_id", nargs="+", type=int, help="A single taxon ID or a list of taxon IDs")
    
    parser.add_argument("--username", type=str, required=False, help="iNaturalist email address")
    parser.add_argument("--password", type=str, required=False, help="iNaturalist password")    
    
    parser.add_argument("-q", "--quality_grade", type=str, choices=("any", "research"), default="any", help="Filter observations by quality grade")
    parser.add_argument("-p", "--per_page", type=int, default=100, help="Number of observations to return per page")
    parser.add_argument("-t", "--threads", type=int, default=10, help="Number of threads to use")
    parser.add_argument("-s", "--sleep", type=float, default=0.0, help="Number of seconds to sleep between requests")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print progress to stdout")

    return parser.parse_args()


def create_logger():
    logger = logging.getLogger("INaturalistExporter")
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

def create_export_dir(export_dir : str = EXPORT_DIR):
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)
    

if __name__ == "__main__":
    args = get_args()

    # create logger
    create_logger()

    # create export directory
    create_export_dir()
    
    # create exporter
    exporter = INaturalistExporter(args.username, args.password)
    
    exporter.export(args.taxon_id)