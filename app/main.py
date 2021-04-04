import requests
import os
from arango_client import Arango
from req_body import request_body
from arango.exceptions import DocumentInsertError
from suburbs import suburbs
from telegram_client import Telegram

HEADERS = {"X-Api-Key": os.getenv("DOMAIN_API_KEY")}
MAX_PER_PAGE = 200
data = []

telega = Telegram()
arango = Arango()


def fetch(page):
    """
    Fetch data from Domain API
    @param page: page number to fetch
    @return: None
    """
    request_body["pageNumber"] = page
    response = session.post(
        "https://api.domain.com.au/v1/listings/residential/_search", json=request_body,
    )
    data.extend(response.json())
    total_pages = divmod(int(response.headers.get("x-total-count")), MAX_PER_PAGE)[0]
    if (total_pages > 0) and (total_pages != page - 1):
        fetch(page=page + 1)


def update_collection():
    """
    Push data to Arango
    @return: None
    """
    for entry in data:
        if entry["type"] == "PropertyListing":
            entry = entry["listing"]
            entry.update({"_key": str(entry["id"])})
            try:
                arango.collection.insert(entry)
            except DocumentInsertError:
                continue
            if not os.getenv("DISABLE_TELEGRAM"):
                telega.client.send_message(
                    os.getenv("RECEIVER"),
                    "https://www.domain.com.au/" + entry["listingSlug"],
                )


if __name__ == "__main__":
    session = requests.Session()
    session.headers.update(HEADERS)
    request_body["pageSize"] = MAX_PER_PAGE
    for k, v in suburbs.items():
        request_body["locations"] += [
            {
                "state": "NSW",
                "suburb": k,
                "postCode": v,
                "includeSurroundingSuburbs": "true",
            }
        ]
    fetch(page=1)
    update_collection()