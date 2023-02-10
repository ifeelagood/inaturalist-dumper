import os
import argparse
import sqlite3
import logging
import zipfile
import requests
import tqdm
import pandas as pd

from common import *

CHUNK_SIZE = 65536
VERNACULAR = "english"

# create image directory
if not os.path.exists(IMAGE_DIR):
    os.mkdir(IMAGE_DIR)
    logging.info(f"Created image directory {IMAGE_DIR}")
    
if not os.path.exists(TAXON_DATABASE):
    if not os.path.exists("inaturalist-taxonomy.dwca.zip"):
        logging.info("Downloading taxonomy database...")
        
        # download taxonomy database
        response = requests.get("https://www.inaturalist.org/taxa/inaturalist-taxonomy.dwca.zip", stream=True)
        total_size = int(response.headers.get("content-length", 0))
        
        with tqdm.tqdm(total=total_size, unit="iB", unit_scale=True) as pbar:
            with open("inaturalist-taxonomy.dwca.zip", "wb") as f:
                for data in response.iter_content(CHUNK_SIZE):
                    pbar.update(len(data))
                    f.write(data)
                    

    # get the vernacular names from the taxonomy database    
    with zipfile.ZipFile("inaturalist-taxonomy.dwca.zip") as zf:            
        with zf.open(f"VernacularNames-{VERNACULAR}.csv") as f:
            vernacular = pd.read_csv(f)
            
    # only keep the columns "id" and "vernacularName"
    vernacular = vernacular[["id", "vernacularName"]]
    
    # rename to "taxon_id" and "name"
    vernacular.columns = ["taxon_id", "name"]
    
    # write to database
    conn = sqlite3.connect(TAXON_DATABASE)
    vernacular.to_sql("taxa", sqlite3.connect(TAXON_DATABASE), index=False, if_exists="replace")
    conn.commit()
    conn.close()

    logging.info("Wrote vernacular to taxonomy database.")
    
