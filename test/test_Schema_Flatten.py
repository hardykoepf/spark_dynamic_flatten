import json
import pytest
from spark_dynamic_flatten.tree_manager import TreeManager
from .utils import relative_to_absolute

@pytest.fixture
def tm_root():
    root = TreeManager.from_schema_json_file(relative_to_absolute("data/formula1_schema.json"))
    return root

def test_get_children(tm_root):
    children = tm_root.get_children()
    assert children is not None

def test_get_tree_layered(tm_root):
    layered = tm_root.get_tree_layered()
    assert (len(layered)) == 5
    # On first index [0] should be 2 nodes defined in testdata
    assert len(layered[0]) == 2
    # Names of nodes in index 1 should be "season" and "teams"
    for node in layered[0]:
        assert node.get_name() in ["season", "teams"]
    # Test data has 5 layers defined
    assert len(layered) == 5

def test_get_tree_as_list(tm_root):
    tree_list = tm_root.get_tree_as_list()
    assert len(tree_list) == 15
    assert tree_list[0] == ('season', 'integer', True, None, None)

def test_generate_fully_flattened_paths(tm_root):
    tree_dict = tm_root.generate_fully_flattened_paths()
    assert len(tree_dict["field_paths"]) == 11

@pytest.fixture
def write_flatten_json(tm_root):
    tm_root.save_fully_flattened_json("test/data/", "formula1_flatten.json")

def test_flatten(write_flatten_json):
    root = TreeManager.from_flatten_json_file("test/data/formula1_flatten.json")
    # TreeManager imports the file again as tree. So children of root node has again to be 2 (season, teams)
    assert len(root.get_children()) == 2