from charset_normalizer import from_path


def detect_encoding(file_path: str) -> str:
    try:
        result = from_path(file_path).best()
        return result.encoding if result else "utf-8"
    except Exception as error:
        print(f"Can not detect encoding for {file_path}: {error}")
        return "utf-8"
