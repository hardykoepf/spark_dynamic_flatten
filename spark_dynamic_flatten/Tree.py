"""Module providing a generic tree implementation."""

from typing import List, Tuple, Optional, Union

class Tree:
    """
    A generic Tree class for representing hierarchical data structures.
    Attributes
    ----------
    self._name : name of node
    self._children : list with reference to child tree nodes
    self._parent : reference to the parent tree node
    self._ancestors_list : list with reference to all ancestors in ordered sequence

    Methods
    -------
    add_child()
        Add a child node
    set_parent()
        Set the parent node
    is_leaf()
        Check if the node is a leaf node
    get_root()
        Returns the root of the tree of node
    is_root()
        Check if node is root node
    get_leafs()
        Returns all leafs of the tree
    get_tree_as_list()
        Returns the tree as nested list.
        Every index of the outer list holds the reference to tree nodes of that layer.
    equals()
        Checks if two trees are equal.
        If not, the difference is also returned as second part of tuple
    search_node_by_path()
        Searches the "nearest" existing node of a path and returns the "nearest" node
        and the missing part of the path
    add_path_to_tree()
        Adds a path (pigeonhole) to the tree
    print()
        Prints the tree
    build_ancestors_list()
        Builds the list of all ancestors for one specific node
    get_path_to_node()
        Returns the path to one specific node. Separator can be choosen.
    subtract(self, other: 'Tree') -> 'Tree':
            Subtracts another Tree from this Tree and returns the difference as a new Tree.  
    """

    # Constant Character for wildcard
    WILDCARD_CHAR = "*"

    def __init__(self,
                 name:str = 'root',
                 parent:Optional['Tree'] = None,
                 children:Optional[List['Tree']] = None
                ):
        """
        Parameters
        ----------
        name : str
            Name of the node. (Default : root)
        parent : Tree
            Reference to the parent node
        children : list[Tree]
            List of references to the children
        """
        self._name = name
        self._children = []
        self._parent = parent
        self._ancestors_list = None

        # When parent was supplied, the new node has to be of same type like parent 
        if parent:
            assert type(self) == type (parent), "Types of pytestparent has to be of same type."

        if children is not None:
            for child in children:
                assert type(self) == type (child), "Types of child has to be of same type."
                self.add_child(child)

    def __repr__(self):
        return repr(self._name)

    def __eq__(self, other) -> bool:
        return bool(repr(self) == repr(other))
    
    def __hash__(self):
        return hash(self._key())
    
    def _key(self):
        return (self._name, id(self), "".join(str(x) for x in self.get_ancestors_list()))

    def set_name(self, name:str):
        """
        Sets/overwrites the name for the node

        Parameters
        ----------
        name: str : Name
        """
        self._name = name

    def get_name(self) -> str:
        """
        Returns the name of the node

        Returns
        ----------
        str : Name of the node
        """
        return self._name

    def get_children(self) -> List["Tree"]:
        """
        Returns the list of children for the node

        Returns
        ----------
        List["Tree"] : List with childrens
        """
        return self._children

    def add_child(self, node:"Tree") -> None:
        """
        Adds a node to the list of children

        Parameters
        ----------
        node : Tree
            Reference to the child node
        """
        assert isinstance(node, Tree), "Node has to be an instance of Tree"
        assert type(self) == type (node), "Types of nodes has to be of same type."
        if node not in self._children:
            # Only add child when not already done
            self._children.append(node)
        # Also check if the parent is already set.
        if node.get_parent() is None:
            node.set_parent(self)

    def get_parent(self) -> Union["Tree", None]:
        """
        Returns the parent of a node

        Returning
        ----------
        Union["Tree", None] : Parent node
        """
        return self._parent

    def set_parent(self, node:"Tree"):
        """
        Sets the parent of a node

        Parameters
        ----------
        node : Tree
            Reference to the parent node
        """
        assert isinstance(node, Tree)
        assert type(self) == type (node), "Types of nodes has to be of same type."
        assert self._parent is None
        self._parent = node
        node.add_child(self)

    def get_root(self) -> "Tree":
        """
        Returns the root of tree to which the node is related

        Returning
        ----------
        "Tree" : Root node
        """
        if self.get_parent() is None:
            return self
        else:
            root = self.get_parent().get_root()
            return root

    def is_leaf(self) -> bool:
        """
        Checks if the node is a leaf

        Returns
        ----------
        bool
        """
        if len(self._children) == 0:
            return True
        else:
            return False

    def is_root(self) -> bool:
        """
        Checks if the node is the root

        Returns
        ----------
        bool
        """
        if self._parent is None:
            return True
        else:
            return False

    def _get_leafs(self, node:"Tree", leafs:Optional[List] = None) -> List["Tree"]:
        # Attention: Here we are not working with copies of the list.
        # We are working with one central list and handing over the pointers!
        if node.get_name() == "root":
            leafs = []
        if node.is_leaf():
            leafs.append(node)
        for child in node.get_children():
            leafs = self._get_leafs(child, leafs)
        return leafs

    def get_leafs(self) -> List:
        """
        Returns the leafs of the tree as list

        Returns
        ----------
        list
            List with all references to the leaf nodes
        """
        # Make sure to start from root
        root = self.get_root()
        return self._get_leafs(root)

    def get_leafs_as_paths(self) -> List[str]:
        """
        Returns a list of paths to all leafs in tree

        Returns
        ----------
        list
            List with all paths to the leaf nodes
        """
        # Get paths to leafs of tree
        return [leaf.get_path_to_node(".") for leaf in self.get_leafs()]

    def _get_tree_as_list(self, node:"Tree", tree_list:Optional[List] = None) -> List:
        # Attention: Here we are not working with copies of the list.
        # We are working with one central list and handing over the pointers!
        if node.get_name() == "root":
            # Ignore root node because its no "real" node
            tree_list = []
        else:
            tree_list.append(node.get_path_to_node("."))
        for child in node.get_children():
            tree_list = self._get_tree_as_list(child, tree_list)
        return tree_list

    def get_tree_as_list(self) -> List:
        """
        Returns the tree as list. Every single node is one list entity with it's path.
        Mainly needed for comparing trees.

        Returns
        ----------
        list
            List with every node path of the tree.
        """
        root = self.get_root()
        return self._get_tree_as_list(root)

    def equals(self, other:"Tree") -> bool:
        """
        Checks if the complete tree equals another tree (not only nodes!).

        Returns
        ----------
        bool
            The bool returns True if trees are identically,
            when trees are not identical, you can use subtract to get differences
        """
        if not isinstance(other, Tree):
            raise TypeError("Type mismatch: both objects must be of type Tree for comparison.")
        list_self = self.get_tree_as_list()
        set_self = set(list_self)
        list_other = other.get_tree_as_list()
        set_other = set(list_other)

        difference = set_self.symmetric_difference(set_other)
        if len(difference) == 0:
            return True
        else:
            return False

    def subtract(self, other:"Tree") -> "Tree":
        """
        Subtracts two trees and returns the difference also as a Tree.
        Trees has to be of same class.

        Returns
        ----------
        Tree
            The bool returns True if trees are identically,
            the set returns differences (set will be empty when identical)
        """
        if not isinstance(other, Tree):
            raise TypeError("Type mismatch: both objects must be of type Tree for subtraction.")
        
        # Convert both trees to sets of tuples
        set_self = set(self._tree_to_tuples(self))
        set_other = set(self._tree_to_tuples(other))

        # Calculate the difference
        difference = set_self - set_other

        # Convert the difference back to a SchemaTree
        if difference:
            return self._tuples_to_tree(difference)
        else:
            return Tree("root")

    def symmetric_difference(self, other:"Tree") -> "Tree":
        """
        Identifies differences comparing two trees and returns the difference also as a Tree.
        Trees has to be of same class.

        Returns
        ----------
        Tree
            The bool returns True if trees are identically,
            the set returns differences (set will be empty when identical)
        """
        if not isinstance(other, Tree):
            raise TypeError("Type mismatch: both objects must be of type Tree for subtraction.")
        
        # Convert both trees to sets of tuples
        set_self = set(self._tree_to_tuples(self))
        set_other = set(self._tree_to_tuples(other))

        # Calculate the difference
        difference = set_self.symmetric_difference(set_other)

        # Convert the difference back to a SchemaTree
        if difference:
            return self._tuples_to_tree(difference)
        else:
            return Tree("root")

    def _search_node_by_name(self, node, name:str) -> Union["Tree",None]:
        if node.get_name() == name:
            return node
        else:
            for child in node.get_children():
                result = self._search_node_by_name(child, name)
                if result is not None:
                    return result
            return None

    def search_node_by_name(self, name:str) -> Union["Tree",None]:
        """
        Searches the node by given name.
        Attention: When the tree has more than one node with same name
        it will only return first found node!

        Parameters
        ----------
        name : str
            Name of the searched node
        
        Returns
        ----------
        Union(Tree, None)
            When a node was found, the node will be returned.
            If nothing was found it will return None
        """
        # Make sure to start from root
        root = self.get_root()
        return self._search_node_by_name(root, name)

    def _search_node_by_path(self, node:"Tree", path_list:List[str]) -> Tuple["Tree", List[str]]:
        if node.get_name() == path_list[0]:
            # I'm the next searched node. So remove my name from path and look if ther
            # is one of my children which we are searching
            temp_list = path_list.copy()
            temp_list.pop(0)
            if len(temp_list) > 0:
                # We are searching a deeper node - ask the children
                for child in node.get_children():
                    child_node, returned_list = self._search_node_by_path(child, temp_list)
                    if len(returned_list) < len(temp_list):
                        # The child was part of the searched branch
                        return child_node, returned_list
            # There was no child which fits better to the searched path.
            # So I'm the best guess by my own
            return node, temp_list
        else:
            # This node is wrong
            assert node.get_parent() is not None, "Seems that root was not first node of the path"
            return node.get_parent(), path_list

    def search_node_by_path(self, path:Union[List[str], str]) -> Tuple["Tree", List[str]]:
        """
        Searches for the nearest node in a tree.
        E.g.: When the tree has following hierarchy stored "node1->node12" and we search for path
        "node1->node12->node123->node1234" we get reference to "node12" (nearest found node)
        and the missing part of the path "node123->node1234" as result.

        Parameters
        ----------
        path : Union[List[str], str]
            Searched path as list (already splitted), where the list has to be in order by layers
            or
            string separated by "." -> Like "node1.node2.node3"
        
        Returns
        ----------
        tuple(Tree, list[str])
            The Tree is the nearest found node, the list includes the missing part of the path
        """
        # Make sure to start from root
        root = self.get_root()
        # Because root is no "real" node, we have to add it to the path (when not already there)
        # before starting logic to search
        if isinstance(path, str):
            temp_list = path.split(".")
        else:
            temp_list = path.copy()
        if root != temp_list[0]:
            temp_list.insert(0, root.get_name())
        # Start to search for best node which is next to the searched path
        next_node, missed_nodes = self._search_node_by_path(root, temp_list)
        return next_node, missed_nodes

    def add_path_to_tree(self, path:str) -> None:
        """
        Adds a path (pigeonhole) to the tree

        Parameters
        ----------
        path : str
            Path to be pigeonholed to the tree
        """
        # Split path
        path_list = path.split(".")
        # Search if the complete path is already existing.
        # If not, we get back the last existing node and the missing part of path
        nearest_node, missing_path = self.search_node_by_path(path_list)
        if len(missing_path) > 0:
            for missing_node in missing_path:
                # Create new node
                if missing_node == missing_path[-1]:
                    # This is a leaf - so we have to add also the alias to the leaf
                    new_node = Tree(missing_node, parent = nearest_node)
                else:
                    new_node = Tree(missing_node, parent = nearest_node)
                nearest_node.add_child(new_node)
                # For next iteration set "nearest_node" to actually created new_node
                nearest_node = new_node

    def _print_tree(self, node:"Tree", layer:int = 0):
        layer_int = layer
        count = 1
        output = ""
        while count < layer_int:
            output = output + "|   "
            count = count + 1
        if layer_int == 0:
            print(repr(node))
        elif count == layer_int:
            print(f"{output}|-- {repr(node)}")
        for child in node.get_children():
            self._print_tree(child, layer_int+1)

    def print(self):
        """
        Prints the tree
        """
        # Be sure to start from root node
        root = self.get_root()
        self._print_tree(root)

    def build_ancestors_list(self) -> None:
        """
        Builds the list of ancestors for a node

        Parameters
        ----------
        path : str
            Path to be pigeonholed to the tree
        """
        if self._ancestors_list is not None:
            return self._ancestors_list

        if self.get_parent() is None or self.get_parent().get_name() == "root":
            # root doesn't have ancestors
            # and first "real node" layer should not have root as ancestor because root is no "real" node
            self._ancestors_list = []
            return self._ancestors_list

        parent = self.get_parent()
        returned_list = parent.get_ancestors_list()
        # Copy the returned list from parent, otherwise we change the list of "pointer"
        self._ancestors_list = returned_list.copy()
        self._ancestors_list.append(self._parent)
        return self._ancestors_list

    def get_ancestors_list(self) -> List["Tree"]:
        """
        Returns the ancestors of a node as ordered list

        Returning
        ----------
        List["Tree"] : Odered List of ancestors. Direct ancestor is at end of list
        """
        if self._ancestors_list is None:
            self.build_ancestors_list()
        return self._ancestors_list

    def get_path_to_node(self, split_char: str) -> str:
        """
        Returns the path to a node separated by choosen split character

        Parameters
        ----------
        split_char : str
            Character used for dividing the nodes
        """
        return "".join(f"{parent.get_name()}{split_char}" for parent in self.get_ancestors_list()) + self.get_name()

    def _get_tree_layered(self, node:"Tree", layer:int = 0, layer_list:Optional[List["Tree"]] = None) -> List["Tree"]:
        if node.is_root():
            # When root create empty list. Root is no "real" node and has to be ignored.
            layer_list = []
        else:
            layer_list = layer_list.copy()
        if node.get_parent() is not None:
            # Only when not root entry
            # Check if list index exist
            if len(layer_list) >= layer+1:
                layer_list[layer].append(node)
            else:
                # first entry of list index - insert nested list
                layer_list.append([node])
            layer = layer +1

        for child in node.get_children():
            layer_list = self._get_tree_layered(child, layer, layer_list)
        return layer_list

    def get_tree_layered(self) -> List["Tree"]:
        """
        Returns the tree in a layered way. Means that references to nodes on same level are grouped in a nested list.
        The index of the outer list represents one layer of the node.

        Returns
        ----------
        list[list[Tree]]
            Every layer of the tree represents one inner list
        """
        root = self.get_root()
        return self._get_tree_layered(root)

    def _tree_to_tuples(self, node: 'Tree') -> List[Tuple]:
        """
        Converts a Tree to a list of tuples representing the tree structure.

        Parameters
        ----------
        node : Tree
            The root-node of tree to convert.

        Returns
        -------
        List[Tuple]
            A list of tuples representing the tree structure.
        """
        if node.get_name() == "root":
            tuples = []
        else:
            path = node.get_path_to_node(".")
            tuples = [(path)]

        for child in node.get_children():
            tuples.extend(self._tree_to_tuples(child))
        return tuples

    def _tuples_to_tree(self, tuples: List[Tuple]) -> 'Tree':
        """
        Converts a list of tuples back to a Tree.

        Parameters
        ----------
        tuples : List[Tuple]
            A list of tuples representing the tree structure.

        Returns
        -------
        Tree
            The reconstructed Tree.
        """
        if not tuples:
            return None

        # Create a root node
        root = Tree("root")

        # Add child nodes
        for path in tuples:
            root.add_path_to_tree(path = path)
        return root

    def to_tree(self) -> 'Tree':
        """
        Converts a FlattenTree or SchemaTree to basic Tree.

        Parameters
        ----------
        node : SchemaTree or FlattenTree
            The root-node of tree to convert.

        Returns
        -------
        Tree
            A tree of type Tree.
        """
        root_node = self.get_root()
        assert issubclass(type(root_node), Tree), "Only subclasses of Tree could be converted to generic Tree"

        new_tree = Tree("root")

        for child_orig in root_node.get_children():
            self._to_tree(child_orig, new_tree)
        return new_tree

    def _to_tree(self, node, parent: 'Tree' = None) -> 'Tree':
        new_node = Tree(node.get_name())
        new_node.set_parent(parent)
        for child in node.get_children():
            self._to_tree(child, new_node)
