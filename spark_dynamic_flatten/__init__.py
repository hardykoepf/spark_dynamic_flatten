import importlib.metadata

# import submodules/classes for easier import
from .tree import Tree
from .tree_schema import SchemaTree
from .tree_flatten import FlattenTree
from .tree_manager import TreeManager
from .flatten import Flatten

try:
    VERSION = importlib.metadata.version(__package__ or __name__)
    __version__ = VERSION
except importlib.metadata.PackageNotFoundError:
    pass
