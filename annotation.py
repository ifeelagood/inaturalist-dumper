import argparse
import asyncio
import logging
import os
import sqlite3

import aiofiles
import aiohttp
import aiosqlite
import aiohttp_socks

from common import *

def get_observation_ids(conn : sqlite3.Connection) -> list:
    """Get all observation ids from the database."""

    c = conn.cursor()
    c.execute("SELECT id FROM observations WHERE annotations IS NULL")
    observation_ids = c.fetchall() 

    return [observation_id[0] for observation_id in observation_ids]


async def scrape_annotations(session : aiohttp.ClientSession, conn : aiosqlite.Connection, queue : asyncio.Queue):
    """Scrape the annotations from an observation."""

    while True:
        
        observation_id = await queue.get()

        try:
            async with session.get(f"https://api.inaturalist.org/v1/observations/{observation_id}", params={"locale": "en"}) as r:
                if r.status == 200:
                    data = await r.json()
                    if data["total_results"] == 1:

                        observation = data["results"][0]

                        annotations = [a["controlled_value"]["label"] for a in observation.get("annotations", [])]
                    
                        if len(annotations) > 0:
                            annotations = ",".join(annotations)

                            print(f"Scraped {observation_id} - {annotations}")
                            await conn.execute("UPDATE observations SET annotations=? WHERE id=?", (annotations, observation_id))
                            await conn.commit()

            
                    else:
                        logging.info(f"Failed to scrape annotations for {observation_id}: {data['total_results']} results.")
                        await queue.put(observation_id)
                else:
                    logging.info(f"Failed to scrape annotations for {observation_id}: {r.status}")
                    await queue.put(observation_id)

        except Exception as e:
            logging.info(f"Failed to scrape annotations for {observation_id}: {e}")
            await queue.put(observation_id)
            
        queue.task_done()

async def scrape_annotations_from_ids(ids : list, limit : int = 10, proxy : str = None):
    """Scrape the annotations from a list of observation ids."""

    conn = await aiosqlite.connect(DATABASE)

    # set the proxy and limit
    connector = aiohttp_socks.ProxyConnector.from_url(proxy, limit=limit)

    session = aiohttp.ClientSession(connector=connector)

    queue = asyncio.Queue()

    for observation_id in ids:
        await queue.put(observation_id)

    await scrape_annotations(session, conn, queue)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30, help="Maximum connections")
    parser.add_argument("--proxy", type=str, default="http://pwsblrrt-rotate:2xwbkzsuxisu@p.webshare.io:80", help="proxy url to use when scraping.")
    
    args = parser.parse_args()

    # create the image directory if it doesn't exist
    if not os.path.exists(IMAGE_DIR):
        os.makedirs(IMAGE_DIR)

    # create the export directory if it doesn't exist
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

    # create the log directory if it doesn't exist
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # create the database connection
    conn = sqlite3.connect(DATABASE)

    # get a list of all observation ids
    ids = get_observation_ids(conn)

    # scrape the annotations for all observation ids
    annotations = asyncio.run(scrape_annotations_from_ids(ids, args.limit, args.proxy))

    # write the annotations to the database
    conn.executemany("UPDATE observations SET annotations=? WHERE id=?", annotations)
    conn.commit()

    logging.info(f"Wrote {len(annotations)} annotations to database.")

    # close the database connection
    conn.close()

if __name__ == "__main__":


    logging.basicConfig(filename=os.path.join(LOG_DIR, "annotations.log"), level=logging.INFO, format="%(asctime)s %(message)s")
    main()
