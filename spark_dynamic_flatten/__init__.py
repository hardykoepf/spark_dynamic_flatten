import importlib.metadata

try:
    VERSION = importlib.metadata.version(__package__ or __name__)
    __version__ = VERSION
except importlib.metadata.PackageNotFoundError:
    pass