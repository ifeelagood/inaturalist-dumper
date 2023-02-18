import os
import argparse
import zipfile
import logging
import requests
import json
import time
import threading
import sqlite3

import aiohttp
import asyncio
import aiofiles
import tqdm


import pandas as pd

from common import *

IMAGE_SIZES = ("small", "medium", "large", "original")


def load_observations(export_dir : str = EXPORT_DIR):
    dataframes = []

    for file in os.listdir(export_dir):
        if file.endswith(".zip"):
            # read the csv file
            with zipfile.ZipFile(os.path.join(export_dir, file)) as zf:
                with zf.open(file.rstrip(".zip")) as f:
                    df = pd.read_csv(f)
                
            # add the dataframe to the list
            dataframes.append(df)
        
    # concatenate all dataframes
    df = pd.concat(dataframes) 

    # drop duplicates
    df.drop_duplicates(inplace=True)
    
    # add empty column annotations
    df["annotations"] = None

    # write to database
    conn = sqlite3.connect(DATABASE)
    df.to_sql("observations", conn, index=False, if_exists="replace")

    conn.commit()
    conn.close()
    
    logging.info(f"Wrote {len(df)} observations to database.")



def get_urls(conn, image_size : str, force : bool = False) -> list:
    urls = []

    # iterate over all observations
    for image_id, taxon_id, image_url in conn.execute("SELECT id, taxon_id, image_url FROM observations"):
        name, ext = os.path.splitext(os.path.basename(image_url))
        
        # check for file extensions
        if ext == ".gif":
            continue
        if ext == ".":
            ext = ".jpg"
            
        
        if not os.path.exists(os.path.join(IMAGE_DIR, f"{image_id}{ext}")) or force:
            image_url = image_url.replace("medium", image_size)

            urls.append((image_id, taxon_id, image_url))
        
    return urls


async def download_image(session : aiohttp.ClientSession, image_id : int, taxon_id : int, image_url : str):
    try:
        async with session.get(image_url) as resp:
            if resp.status == 200:
                ext = os.path.splitext(os.path.basename(image_url))[1]
                
                async with aiofiles.open(os.path.join(IMAGE_DIR, f"{image_id}{ext}"), "wb") as f:
                    await f.write(await resp.read())
                    
                    logging.info(f"Saved {image_id} for taxon {taxon_id}.")
                
            else:
                logging.info(f"Failed to download {image_id} for taxon {taxon_id}: {resp.status}")
    except Exception as e:
        logging.info(f"Failed to download {image_id} for taxon {taxon_id}: {e}")

async def download_images(urls : list, image_size : str, semaphore : int = 10):
    async with aiohttp.ClientSession() as session:
        tasks = []
        
        # create a semaphore to limit the number of concurrent downloads
        sem = asyncio.Semaphore(semaphore)
        
        for image_id, taxon_id, image_url in urls:
            # wait until the semaphore is available
            async with sem:
                tasks.append(asyncio.create_task(download_image(session, image_id, taxon_id, image_url)))
                
        await asyncio.gather(*tasks)
    
    logging.info(f"Downloaded {image_size} images.")


def init_logging():
    # create the log directory if it doesn't exist
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        
    # initialize the logger
    logging.basicConfig(filename=os.path.join(LOG_DIR, "scrape.log"), level=logging.INFO, format="%(asctime)s %(message)s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape images from iNaturalist exports.")
    parser.add_argument("-s", "--size", type=str, default="medium", choices=IMAGE_SIZES, help="Image size to download.")
    parser.add_argument("-f", "--force", action="store_true", help="Overwrite existing images.")
    parser.add_argument("--semaphore", type=int, default=10, help="Number of concurrent downloads.")
    
    args = parser.parse_args()
    
    # create the image directory if it doesn't exist
    if not os.path.exists(IMAGE_DIR):
        os.makedirs(IMAGE_DIR)
        
    # initialize the logger
    init_logging()
        
    # load the observations
    load_observations()

    # connect to the database
    conn = sqlite3.connect(DATABASE)

    # get the urls
    urls = get_urls(conn, args.size, args.force)
    
    # close the connection
    conn.close()
    
    # download the images
    asyncio.run(download_images(urls, args.size, args.semaphore))
