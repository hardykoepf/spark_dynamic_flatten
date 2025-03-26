import importlib.metadata

# import submodules/classes for easier import
from spark_dynamic_flatten.tree import Tree
from spark_dynamic_flatten.tree_schema import SchemaTree
from spark_dynamic_flatten.tree_flatten import FlattenTree
from spark_dynamic_flatten.tree_manager import TreeManager
from spark_dynamic_flatten.flatten import Flatten

try:
    VERSION = importlib.metadata.version(__package__ or __name__)
    __version__ = VERSION
except importlib.metadata.PackageNotFoundError:
    pass
