import os
import requests
from tqdm import tqdm


def download_file(url: str, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    file_name = os.path.join(output_dir, url.split("/")[-1])
    response = requests.get(url=url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024

    with open(file_name, "wb") as file, tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc=os.path.basename(file_name),
            ncols=100,
    ) as progress_bar:
        for chunk in response.iter_content(block_size):
            file.write(chunk)
            progress_bar.update(len(chunk))

    return file_name
