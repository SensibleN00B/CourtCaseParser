import json
import os

import requests
from dotenv import load_dotenv


def fetch_dataset_metadata(dataset_id: str) -> dict:
    url = f"https://data.gov.ua/api/3/action/package_show?id={dataset_id}"
    response = requests.get(url=url)
    response.raise_for_status()
    data = response.json()
    if not data.get("success"):
        raise RuntimeError("Was returned unsuccessful response from API")

    return data["result"]


def extract_resources(metadata: dict) -> list[dict]:
    resources = metadata.get("resources", [])
    extracted = []

    for res in resources:
        extracted.append(
            {
                "name": res.get("name"),
                "description": res.get("description"),
                "format": res.get("format"),
                "url": res.get("url"),
            }
        )

    return extracted


if __name__ == "__main__":
    load_dotenv()
    dataset_id = os.getenv("DATASET_ID")
    resources = extract_resources(fetch_dataset_metadata(dataset_id))

    print(f"Found {len(resources)} resources")
    print(json.dumps(resources, indent=2, ensure_ascii=False))
