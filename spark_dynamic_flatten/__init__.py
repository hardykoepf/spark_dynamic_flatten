import importlib.metadata

# import submodules/classes for easier import
from .flatten import Flatten
from .tree import Tree
from .tree import SchemaTree
from .tree import FlattenTree
from .tree_manager import TreeManager

try:
    VERSION = importlib.metadata.version(__package__ or __name__)
    __version__ = VERSION
except importlib.metadata.PackageNotFoundError:
    pass
