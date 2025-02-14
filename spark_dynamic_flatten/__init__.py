import importlib.metadata
# import submodules for easier import
import spark_dynamic_flatten.flatten
import spark_dynamic_flatten.tree
import spark_dynamic_flatten.tree_manager

try:
    VERSION = importlib.metadata.version(__package__ or __name__)
    __version__ = VERSION
except importlib.metadata.PackageNotFoundError:
    pass
