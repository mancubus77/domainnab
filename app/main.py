import requests
import os
import time
from datetime import datetime
from arango_client import Arango
from req_body import request_body
from suburbs import suburbs
from telegram_client import Telegram
import logging
import sys
from mongo_client import Mongo
from pgsql import Psql

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting app...")
HEADERS = {"X-Api-Key": os.getenv("DOMAIN_API_KEY")}
MAX_PER_PAGE = 200
data = []

telega = Telegram()
arango = Arango()
# mongo = Mongo()
# psql = Psql()


def fetch(page):
    """
    Fetch data from Domain API
    @param page: page number to fetch
    @return: None
    """
    request_body["pageNumber"] = page
    if len(data) < 1000:
        response = session.post(
            "https://api.domain.com.au/v1/listings/residential/_search", json=request_body,
        )
        data.extend(response.json())
        total_pages = divmod(int(response.headers.get("x-total-count")), MAX_PER_PAGE)[0]
        if (total_pages > 0) and (total_pages != page - 1):
            fetch(page=page + 1)
        logger.info(
            f"X-Quota-PerDay-Remaining: {response.headers.get('X-Quota-PerDay-Remaining')}"
        )
    else:
        logger.warning("Can not exceed 1000 requests. Quitting...")


if __name__ == "__main__":
    logger.info(f"Starting {str(datetime.now())}")
    session = requests.Session()
    session.headers.update(HEADERS)
    request_body["pageSize"] = MAX_PER_PAGE
    for k, v in suburbs.items():
        request_body["locations"] = [
            {
                "state": "NSW",
                "suburb": k,
                "postCode": v,
                "includeSurroundingSuburbs": "true",
            }
        ]
        logger.info(f"Processing: {k}")
        fetch(page=1)
        for entry in data:
            if entry["type"] == "PropertyListing":
                entry = entry["listing"]
                # Mongo
                # mongo.insert_document(
                #     (lambda x: x.update({"ts": int(time.time()), "_id": x["id"]}) or x)(
                #         entry
                #     )
                # )
                # Arango
                is_new = arango.insert_document(
                    (
                        lambda x: x.update({"ts": int(time.time()), "_key": str(x["id"])})
                        or x
                    )(entry)
                )
                # PSQL
                # psql.insert_document(entry)
                if (
                    not os.getenv("DISABLE_TELEGRAM")
                    and is_new
                    and entry["propertyDetails"]["propertyType"] in ["Townhouse", "Villa", "House"]
                ):
                    telega.send_telegram_message(
                        os.getenv("RECEIVER"),
                        format(
                            f'**{entry["propertyDetails"]["propertyType"]}** {entry["priceDetails"]["displayPrice"]}\n'
                            f'https://www.domain.com.au/{entry["listingSlug"]}'
                        ),
                    )
        data = []
