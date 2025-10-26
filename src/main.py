import asyncio
import os
from dotenv import load_dotenv

from csv_to_db import import_csv_files
from fetch_metadata import fetch_dataset_metadata, extract_resources
from resource_downloader import download_all_files
from zip_unpacker import unpack_zip

SUPPORTED_FORMATS = ["csv", "zip"]


def main():
    load_dotenv()
    dataset_id = os.getenv("DATASET_ID")

    print("Fetching dataset metadata...")
    metadata = fetch_dataset_metadata(dataset_id)
    resources = extract_resources(metadata)
    resources = [res for res in resources if res.get("format", "").lower() in SUPPORTED_FORMATS]
    print(f"Found {len(resources)} supported resources")

    data_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(data_dir, exist_ok=True)

    print("Starting download...")
    results = download_all_files(resources, data_dir, max_workers=8)

    print("\nSummary of downloading:")
    for result in results:
        if result["status"] == "success":
            print(f"✅ {result['name']} -> {result['path']}")
        else:
            print(f"❌ {result['name']} -> {result['error']}")

    print("\nUnpacking ZIP files...")
    summary = []
    unpacked_dir = os.path.join(data_dir, "unpacked")
    os.makedirs(unpacked_dir, exist_ok=True)
    for result in results:
        if result["status"] == "success" and result["path"].lower().endswith(".zip"):
            csv_files = unpack_zip(result["path"], unpacked_dir)
            summary.append(
                {
                    "archive": result["name"],
                    "csv_count": len(csv_files),
                    "status": "OK" if csv_files else "Empty or invalid"
                }
            )

    if summary:
        print(f"\nSummary of unpacking:")
        for item in summary:
            print(f"{item['archive']} -> {item['csv_count']} CSV ({item["status"]})")
    else:
        print("No ZIP files to unpack")

    asyncio.run(import_csv_files(unpacked_dir))


if __name__ == "__main__":
    main()
