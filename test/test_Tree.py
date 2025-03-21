import pytest
from spark_dynamic_flatten import Tree

@pytest.fixture
def create_tree():
    root = Tree(name="root")
    previous = root
    for i in range(1,6):
        for j in range(1,i + 1):
            node_name = f"node_{i}_{j}"
            node = Tree(node_name, previous)
            previous.add_child(node)

        previous = node
    return root

@pytest.fixture
def create_dupl():
    root = Tree("root")
    previous = root
    for i in range(1,6):
        for j in range(1,i + 1):
            node_name = f"node_{i}_{j}"
            node = Tree(node_name, previous)
            previous.add_child(node)

        previous = node
    return root

@pytest.fixture
def create_diff():
    root = Tree("root")
    previous = root
    for i in range(1,7):
        for j in range(1,i + 1):
            node_name = f"node_{i}_{j}"
            node = Tree(node_name, previous)
            previous.add_child(node)

        previous = node
    return root

@pytest.fixture
def mid_node(create_tree):
    #Search node in middle of hierarchy with childrens
    return create_tree.search_node_by_name("node_3_3")

@pytest.fixture
def leaf_node(create_tree):
    #Search node in middle of hierarchy with childrens
    return create_tree.search_node_by_name("node_5_3")

def test_search_node_by_path(create_tree, mid_node):
    #Search node by path
    nearest_node, _ = create_tree.search_node_by_path("node_1_1.node_2_2.node_3_3")
    assert nearest_node.get_name() == mid_node.get_name()
    nearest_node, missed_path = create_tree.search_node_by_path("node_1_1.node_2_2.node_3_3.not_existing_node")
    assert nearest_node.get_name() == mid_node.get_name()
    assert len(missed_path) == 1

def test_get_root(mid_node):
    assert mid_node.get_root().get_name() == "root"

def test_get_children(mid_node):
    assert len(mid_node.get_children()) == 4

def test_get_parent(mid_node):
    assert mid_node.get_parent() is not None

def test_add_child(mid_node):
    mid_node.add_child(Tree("new_child", mid_node))
    assert len(mid_node.get_children()) == 5

def test_set_parent(mid_node):
    new_node = Tree("new_node_with_parent")
    new_node.set_parent(mid_node)
    assert new_node.get_parent() is not None
    assert len(mid_node.get_children()) == 5

def test_is_leaf(mid_node, leaf_node):
    assert mid_node.is_leaf() == False
    assert leaf_node.is_leaf() == True

def test_is_root(create_tree, mid_node, leaf_node):
    assert create_tree.is_root() == True
    assert mid_node.is_root() == False
    assert leaf_node.is_root() == False

def test_get_leafs(create_tree):
    assert len(create_tree.get_leafs()) == 11

def test_get_leafs_as_paths(create_tree):
    assert len(create_tree.get_leafs_as_paths()) == 11

def test_get_tree_as_list(create_tree):
    assert len(create_tree.get_tree_as_list()) == 15

def test_equals(create_tree, create_dupl):
    assert create_tree.equals(create_dupl)
    # Make changes to one tree
    create_dupl.add_child(Tree("new"))
    assert create_tree.equals(create_dupl) == False

def test_subtract(create_tree, create_dupl):
    result = create_tree.subtract(create_dupl)
    assert len(result) == 0
    # Make changes to one tree
    create_dupl.add_child(Tree("new"))
    result = create_dupl.subtract(create_tree)
    assert len(result) == 1

def test_add_path_to_tree(create_tree):
    create_tree.add_path_to_tree("season.new_path")

def test_get_path_to_node(mid_node):
    assert mid_node.get_path_to_node("#") == "node_1_1#node_2_2#node_3_3"

def test_get_tree_layered(create_tree):
    layered = create_tree.get_tree_layered()
    assert len(layered) == 5
    assert len(layered[1]) == 2

def test_hashable(create_tree):
    list = create_tree.get_tree_layered()
    tree_set = set(list[2])
    assert len(tree_set) == 3


if __name__ == "__main__":
    pytest.main([__file__,"-s"])