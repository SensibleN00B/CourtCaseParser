import os
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from tqdm import tqdm


def create_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def download_file(url: str, output_dir: str, position: int = 0) -> str:
    os.makedirs(output_dir, exist_ok=True)
    file_name = os.path.join(output_dir, url.split("/")[-1])

    try:
        session = create_session()
        response = session.get(url, stream=True, timeout=150)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        block_size = 1024

        with open(file_name, "wb") as file, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=os.path.basename(file_name),
                ncols=100,
                position=position,
                leave=False,
        ) as progress_bar:
            for chunk in response.iter_content(block_size):
                file.write(chunk)
                progress_bar.update(len(chunk))

        return file_name

    except Exception as error:
        raise RuntimeError(f"An error occurred while loading {url}: {error}")


def download_all_files(resources: list[dict], output_dir: str, max_workers: int = 3):
    os.makedirs(output_dir, exist_ok=True)
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_res = {
            executor.submit(download_file, res["url"], output_dir, index): res
            for index, res in enumerate(resources)
            if res.get("url")
        }

        for future in tqdm(
                as_completed(future_to_res),
                total=len(future_to_res),
                desc="Downloading files...",
                ncols=100
        ):
            res = future_to_res[future]
            try:
                results.append(
                    {
                        "name": res["name"],
                        "path": future.result(),
                        "status": "success",
                    }
                )
            except Exception as error:
                results.append(
                    {
                        "name": res["name"],
                        "error": str(error),
                        "status": "failed",
                    }
                )

        return results
