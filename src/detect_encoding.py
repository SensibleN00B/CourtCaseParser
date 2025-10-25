from charset_normalizer import from_path


def detect_encoding(file_path: str) -> str:
    try:
        result = from_path(file_path).best()
        encoding = result.encoding if result else "utf-8"
    except Exception as error:
        print(f"Can not detect encoding for {file_path}: {error}")
        return "utf-8"

    # try:
    #     with open(file_path, "r", encoding=encoding) as f:
    #         f.read(2000)
    # except UnicodeDecodeError:
    #     print(f"{file_path}: fallback to cp1251")
    #     encoding = "cp1251"

    return encoding
