import pytest
from spark_dynamic_flatten.Tree import Tree, FlattenTree, SchemaTree
from spark_dynamic_flatten.TreeManager import TreeManager
from .utils import relative_to_absolute
import json

@pytest.fixture
def tm():
    tm = TreeManager.from_schema_json_file(relative_to_absolute("data/formula1_schema.json"))
    #tm.get_root_node().print_tree()
    #print(tm.get_root_node().get_tree_layered())
    return tm


@pytest.fixture
def tm_root(tm):
    return tm.get_root_node()

def test_import(tm, tm_root):
    assert tm is not None

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
    tree_json = tm_root.generate_fully_flattened_paths()
    tree_dict = json.loads(tree_json)
    assert len(tree_dict["field-paths"]) == 11