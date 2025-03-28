import os
import json
import pytest
from pyspark.sql.types import *
from spark_dynamic_flatten import TreeManager, Tree, SchemaTree
try:
    from .utils import relative_to_absolute
except ImportError:
    from utils import relative_to_absolute

@pytest.fixture
def tm_root():
    root = TreeManager.from_schema_json_file(relative_to_absolute("data/formula1_schema.json"))
    return root

@pytest.fixture
def tm_root2():
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

def test_to_tree(tm_root):
    tree = tm_root.to_tree()
    assert isinstance(tree, Tree), f"Tree is not of instance Tree: {type(tree)}"
    layered = tree.get_tree_layered()
    assert (len(layered)) == 5
    # On first index [0] should be 2 nodes defined in testdata
    assert len(layered[0]) == 2
    # Names of nodes in index 1 should be "season" and "teams"
    for node in layered[0]:
        assert node.get_name() in ["season", "teams"]
    # Test data has 5 layers defined
    assert len(layered) == 5

def test_subtract(tm_root, tm_root2):
    tm_root2.add_path_to_tree(
        path = "teams.drivers.nickname",
        data_type = "string",
        nullable = True
    )
    difference = tm_root2.subtract(tm_root)
    assert len(difference) == 1
    assert difference == [{'path': 'teams.drivers.nickname', 'data_type': 'string', 'nullable': True, 'element_type': None, 'contains_null': None, 'key_type': None, 'value_type': None}]
    # Only subtract by path -> convert to Tree
    tm_root = tm_root.to_tree()
    tm_root2 = tm_root2.to_tree()
    difference = tm_root2.subtract(tm_root)
    assert len(difference) == 1
    assert difference == [{'path': 'teams.drivers.nickname'}]

@pytest.fixture
def write_flatten_json(tm_root):
    tm_root.save_fully_flattened_json("test/data/", "formula1_flatten.json")

def test_flatten(write_flatten_json):
    root = TreeManager.from_flatten_json_file("test/data/formula1_flatten.json")
    # TreeManager imports the file again as tree. So children of root node has again to be 2 (season, teams)
    assert len(root.get_children()) == 2

def test_flatten_struct(tm_root):
    tree_struct = tm_root.generate_fully_flattened_struct()
    # count fields of StructType 
    assert len(tree_struct.fieldNames()) == 11

@pytest.fixture
def structtype():
    return StructType([StructField('season', IntegerType(), True), StructField('name', StringType(), True), StructField('driver_id', IntegerType(), True), StructField('name#2', StringType(), True), StructField('nationality', StringType(), True), StructField('race_id', IntegerType(), True), StructField('race_name', StringType(), True), StructField('position', IntegerType(), True), StructField('points', IntegerType(), True), StructField('lap_number', IntegerType(), True), StructField('time', StringType(), True)])

def test_generate_flattened_schema(tm_root, structtype):
    flatten = TreeManager.from_flatten_json_file("test/data/formula1_flatten.json")
    file_name = os.path.join(os.path.dirname(os.path.realpath('__file__')), r'test/data/formula1_schema.json')
    with open(file_name, "r") as f:
        struct_file = json.load(f)
    struct = StructType.fromJson(struct_file)
    struct_flat = flatten.generate_flattened_schema(struct)
    assert struct_flat == structtype

def test_to_struct_type(tm_root):
    # Create struct from tm_root schema and convert back to tree.
    new_tree = TreeManager.from_struct_type(tm_root.to_struct_type())
    assert tm_root.equals(new_tree)

@pytest.fixture
def tm_scrambled():
    # Scrambled version of schema for testing subtract and intersection only by names
    root = TreeManager.from_schema_json_file(relative_to_absolute("data/formula1_schema_scrambled.json"))
    return root

def test_intersection(tm_root, tm_scrambled):
    tree_intersection = tm_root.intersection(tm_scrambled)
    layered = tree_intersection.get_tree_layered()
    assert len(layered[1]) == 1
    # nodes/names were NOT changed, so result should be same like tm_root when only comparing path as basic Tree
    tm_root = tm_root.to_tree()
    tm_scrambled = tm_scrambled.to_tree()
    assert tm_root.equals(tm_scrambled)
    tree_intersection = tm_root.intersection(tm_scrambled)
    layered = tree_intersection.get_tree_layered()
    tree_intersection.print()
    assert (len(layered[3]) == 5 and len(layered) == 5 )

if __name__ == "__main__":
    pytest.main([__file__,"-s"])