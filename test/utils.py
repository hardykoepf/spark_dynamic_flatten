import os

def relative_to_absolute(relative_path: str) -> str:
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, relative_path)
    return filename