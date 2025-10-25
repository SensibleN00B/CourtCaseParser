import os
import zipfile


def unpack_zip(zip_path: str, output_dir: str) -> list[str]:
    extracted_files = []
    os.makedirs(output_dir, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)
            for name in zip_ref.namelist():
                if name.lower().endswith(".csv"):
                    extracted_files.append(os.path.join(output_dir, name))
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid ZIP archive ")

    return extracted_files
