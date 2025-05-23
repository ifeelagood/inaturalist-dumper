# iNaturalist Dumper

This script was written to bulk export images of taxa from inaturalist.org. It uses iNaturalist's bulk export HTTP API which is available to logged in users.

# Libraries

It uses requests for single threaded HTTP, and aiohttp and asyncio for asynchronous downloading and writing of metadata.

Metadata is stored in an sqlite3 database, and is written to asynchronously using aiosqlite3

# Usage

First, find the desired taxon id, e.g. 12345. Then run exporter which will export to 12345.zip

`python export.py taxon_id(s) --username USERNAME --password PASSWORD`

Then, run scrape to asynchronously download images and put metadata into an sqlite database.

`python scrape.py --semaphore CONNECTION_LIMIT`

Finally, run annotation to fetch additional metadata for each observation, and update database accordingly.

`python annotation.py --limit CONNECTION_LIMIT --proxy PROXY_URL`
