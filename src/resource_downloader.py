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
        allowed_methods=["GET", "HEAD"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _parse_total_from_content_range(value: str | None) -> int | None:
    if not value or "/" not in value:
        return None
    try:
        return int(value.split("/")[-1])
    except Exception:
        return None


def download_file(url: str, output_dir: str, position: int = 0) -> str:
    os.makedirs(output_dir, exist_ok=True)
    file_name = os.path.join(output_dir, url.split("/")[-1])

    max_attempts = 5
    attempt = 1
    block_size = 1024 * 1024

    session = create_session()

    while attempt <= max_attempts:
        try:
            existing_size = os.path.getsize(file_name) if os.path.exists(file_name) else 0
            headers = {}
            if existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"

            try:
                head = session.head(url, timeout=30, allow_redirects=True)
                if head.ok:
                    cl = head.headers.get("content-length")
                    remote_total = int(cl) if cl and cl.isdigit() else None
            except Exception:
                pass

            response = session.get(url, stream=True, timeout=(10, 300), headers=headers)
            if response.status_code == 416 and existing_size > 0:
                return file_name
            if existing_size > 0 and response.status_code == 200:
                full_length = response.headers.get("content-length")
                if full_length and full_length.isdigit() and int(full_length) == existing_size:
                    return file_name
                response.close()
                try:
                    os.remove(file_name)
                except FileNotFoundError:
                    pass
                existing_size = 0
                headers.pop("Range", None)
                response = session.get(url, stream=True, timeout=(10, 300))

            response.raise_for_status()
            desc = os.path.basename(file_name) + (" (resuming)" if existing_size else "")

            if response.status_code == 206:
                total = _parse_total_from_content_range(response.headers.get("Content-Range"))
                total_size = (total - existing_size) if total is not None else None
            else:
                cl = response.headers.get("content-length")
                total_size = int(cl) if cl and cl.isdigit() else None

            mode = "ab" if existing_size > 0 else "wb"
            written_this_attempt = 0
            with open(file_name, mode) as file, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=desc,
                ncols=100,
                position=position,
                leave=False,
            ) as progress_bar:
                if total_size is None and existing_size:
                    progress_bar.reset(total=0)
                for chunk in response.iter_content(block_size):
                    if not chunk:
                        continue
                    file.write(chunk)
                    written_this_attempt += len(chunk)
                    progress_bar.update(len(chunk))

            return file_name

        except Exception as error:

            attempt += 1
            if attempt > max_attempts:
                raise RuntimeError(f"An error occurred while loading {url}: {error}")
            try:
                response.close()  # type: ignore[name-defined]
            except Exception:
                pass
            continue
        finally:
            try:
                response.close()  # type: ignore[name-defined]
            except Exception:
                pass
    return None


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
