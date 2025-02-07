from pyspark.sql.types import StructType, ArrayType, StructField, StringType, IntegerType, FloatType, BooleanType, DoubleType, LongType, ShortType, ByteType, DateType, TimestampType, DecimalType, BinaryType, NullType, DataType
from typing import List, Tuple, Optional, TypeVar, Union
import json

basic_spark_types = TypeVar(IntegerType, FloatType, BooleanType, DoubleType, LongType, ShortType, ByteType, DateType, TimestampType, DecimalType, BinaryType, NullType, DataType)

class Tree(object):
    """
    Generic Tree 
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
    is_root()
        Check if node is root node
    get_leafs()
        Returns all leafs of the tree
    get_tree_as_list()
        Returns the tree as nested list. Every index of the outer list holds the reference to tree nodes of that layer.
    equals()
        Checks if two trees are equal. If not, the difference is also returned as second part of tuple
    search_node_by_path()
        Searches the "nearest" existing node of a path and returns the "nearest" node and the missing part of the path
    add_path_to_tree()
        Adds a path (pigeonhole) to the tree
    print_tree()
        Prints the tree
    build_ancestors_list()
        Builds the list of all ancestors for one specific node
    get_path_to_node()
        Returns the path to one specific node. Separator can be choosen.
    """
    def __init__(self, name:str = 'root', parent:Optional['Tree'] = None, children:Optional[List['Tree']] = None):
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

        if children is not None:
            for child in children:
                self.add_child(child)

    def __repr__(self):
        return repr(self._name)
    
    def __eq__(self, other) -> bool:
        if repr(self) == repr(other):
            return True
        else:
            return False

    def set_name(self, name):
        self._name = name

    def get_name(self) -> str:
        return self._name

    def get_children(self) -> List["Tree"]:
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
        if node not in self._children:
            # Only add child when not already done
            self._children.append(node)
        # Also check if the parent is already set.
        if node._parent == None:
                node.set_parent(self)

    def get_parent(self) -> "Tree":
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
        assert self._parent == None
        self._parent = node
        node.add_child(self)

    def _get_root(self) -> "Tree":
        if self._parent == None:
            return self
        else:
            root = self._parent._get_root()
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
        if self._parent == None:
            return True
        else:
            return False

    def _get_leafs(self, leafs:Optional[List] = []) -> List["Tree"]:
        # Attention: Here we are not working with copies of the list. We are working with one central list and handing over the pointers!
        if self._name == "root":
            leafs = []
        if self.is_leaf():
            leafs.append(self)
        for child in self._children:
            leafs = child._get_leafs(leafs)
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
        root = self._get_root()
        return root._get_leafs()
    
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
    
    def _get_tree_as_list(self, tree_list:Optional[List] = []) -> List:
        # Attention: Here we are not working with copies of the list. We are working with one central list and handing over the pointers!
        if self._name == "root":
            # Ignore root node because its no "real" node
            tree_list = []
        else:
            tree_list.append(self.get_path_to_node("."))
        for child in self._children:
            tree_list = child._get_tree_as_list(tree_list)
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
        root = self._get_root()
        return root._get_tree_as_list()

    def equals(self, other:"Tree") -> Tuple[bool, set]:
        """
        Checks if the complete tree equals another tree (not only nodes!).

        Returns
        ----------
        tuple(bool, set)
            The bool returns if trees are identically, the set returns differences (empty when identical)
        """
        if type(other) == type(self):
            list_self = self.get_tree_as_list()
            set_self = set(list_self)
            list_other = other.get_tree_as_list()
            set_other = set(list_other)

            difference = set_self.symmetric_difference(set_other)
            if len(difference) == 0:
                return True, difference
            else:
                return False, difference
        else:
            return False, set("Type mismatch")

    def _search_node_by_name(self, name:str) -> Union["Tree",None]:
        if self._name == name:
            return self
        else:
            for child in self._children:
                result = child._search_node_by_name(name)
                if result is not None:
                    return result
            return None

    def search_node_by_name(self, name:str) -> Union["Tree",None]:
        """
        Searches the node by given name.
        Attention: When the tree has more than one naode with same name, it will only return first found node!

        Parameters
        ----------
        name : str
            Name of the searched node
        
        Returns
        ----------
        Union(Tree, None)
            When a node was found, the node will be returned. If nothing was found it will return None
        """
        # Make sure to start from root
        root = self._get_root()
        return root._search_node_by_name(name)

    def _search_node_by_path(self, path_list:List[str]) -> Tuple["Tree", List[str]]:
        if self._name == path_list[0]:
            # I'm the next searched node. So remove my name from path and look if there is one of my children which we are searching
            temp_list = path_list.copy()
            temp_list.pop(0)
            if len(temp_list) > 0:
                # We are searching a deeper node - ask the children
                for child in self._children:
                    child_node, returned_list = child._search_node_by_path(temp_list)
                    if len(returned_list) < len(temp_list):
                        # The child was part of the searched branch
                        return child_node, returned_list
            # There was no child which fits better to the searched path. So I'm the best guess by my own
            return self, temp_list
        else:
            # This node is wrong
            assert self._parent is not None, "Seems that root was not first node of the path"
            return self._parent, path_list

    def search_node_by_path(self, path_list:List[str]) -> Tuple["Tree", List[str]]:
        """
        Searches for the nearest node in a tree.
        E.g.: When the tree has following hierarchy stored "node1->node12" and we search for path
        "node1->node12->node123->node1234" we get reference to "node12" (nearest found node)
        and the missing part of the path "node123->node1234" as result.

        Parameters
        ----------
        path_list : list[str]
            Searched path as list, where the list has to be ordered (root node )
        
        Returns
        ----------
        tuple(Tree, list[str])
            The Tree is the nearest found node, the list includes the missing part of the path
        """
        # Make sure to start from root
        root = self._get_root()
        # Because root is no "real" node, we have to add it (when not already there) to the path before starting logic to search
        temp_list = path_list.copy()
        if root != temp_list[0]:
            temp_list.insert(0, root._name)
        # Start to search for best node which is next to the searched path
        next_node, missed_nodes = root._search_node_by_path(temp_list)
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
        # Search if the complete path is already existing. If not, we get back the last existing node and the missing part of path
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

    def _print_tree(self, layer:int = 0):
        layer = layer
        count = 0
        output = ""
        while count < layer:
            output = output + "|   "
            count = count + 1
        if count == layer:
            print(f"{output}|-- {repr(self)}")
        for child in self._children:
            child._print_tree(layer+1)

    def print_tree(self):
        """
        Prints the tree
        """
        # Be sure to start from root node
        root = self._get_root()
        root._print_tree()

    def _build_ancestors_list(self) -> List["Tree"]:
        if self._ancestors_list != None:
            return self._ancestors_list
        elif self._parent == None or self._parent._name == "root":
            # root doesn't have ancestors
            # and first "real node" layer should not have root as ancestor because root is no "real" node
            self._ancestors_list = []
            return self._ancestors_list
        else:
            returned_list = self._parent._build_ancestors_list()
            # Copy the returned list from parent, otherwise we change the list of "pointer"
            self._ancestors_list = returned_list.copy()
            self._ancestors_list.append(self._parent)
            return self._ancestors_list

    def build_ancestors_list(self) -> None:
        """
        Builds the list of ancestors for a node

        Parameters
        ----------
        path : str
            Path to be pigeonholed to the tree
        """
        # Build list of ancestors if not already done once for this node
        if self._ancestors_list == None:
            self._build_ancestors_list()

    def get_path_to_node(self, split_char: str) -> str:
        # Make sure ancestors list was built
        self._build_ancestors_list()
        return "".join(f"{parent._name}{split_char}" for parent in self._ancestors_list) + self._name

    def _get_tree_layered(self, layer:int = 0, layer_list:Optional[List["Tree"]] = []) -> List["Tree"]:
        layer_list = layer_list.copy()
        if self._parent != None:
            # Only when not root entry
            # Check if list index exist
            if len(layer_list) >= layer+1:
                layer_list[layer].append(self)
            else:
                # first entry of list index - insert nested list
                layer_list.append([self])
            
            layer = layer +1

        for child in self._children:
            layer_list = child._get_tree_layered(layer, layer_list)
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
        root = self._get_root()
        return root._get_tree_layered()


class FlattenTree(Tree):
    """
    Tree for Flatten
    Inherited from Tree

    Attributes
    ----------
    self.alias : Alias name of the node (only for leaf nodes)
    self.is_identifier : Is node key (only for leaf nodes)

    Methods
    -------
    add_path_to_tree()
        Adds a path (pigeonhole) to the tree
    """
    def __init__(self, name:str = 'root', alias:str = None, is_identifier:bool = False, parent:Optional['Tree'] = None, children:Optional[List['Tree']] = None):
        """
        Parameters
        ----------
        alias : str
            Alias name
        is_identifier : bool
            Is the node handled as key to table
        """
        # Call Constructor of super class
        super().__init__(name, parent, children)
        # alias needed for Flattening
        self.alias = alias
        # is_identifier needed for Flattening
        self.is_identifier = is_identifier

    def get_alias(self) -> str:
        return self.alias
    
    def get_is_identifier(self) -> bool:
        return self.is_identifier
    
    def is_son_wildcard(self) -> bool:
        for child in self._children:
            if child._name == "*":
                return True
        return False

    def add_path_to_tree(self, path:str, alias:str = None, is_identifier:bool = False) -> None:
        """
        Adds a path (pigeonhole) to the tree. Overwrite method of super class.
        Tis node also takes care about having alias and is_identifier on leaf nodes

        Parameters
        ----------
        path : str
            Path to be pigeonholed to the tree
        alias : str
            Alias name for this path/field
        is_identifier : bool
            Is the path/field key to target table
        """
        # Split path
        path_list = path.split(".")
        # Search if the complete path is already existing. If not, we get back the last existing node and the missing part of path
        nearest_node, missing_path = self.search_node_by_path(path_list)
        if len(missing_path) > 0:
            for missing_node in missing_path:
                # Create new node
                if missing_node == missing_path[-1]:
                    # This is a leaf - so we have to add also the alias to the leaf
                    new_node = FlattenTree(missing_node, parent = nearest_node, alias = alias, is_identifier = is_identifier)
                else:
                    new_node = FlattenTree(missing_node, parent = nearest_node)
                nearest_node.add_child(new_node)
                # For next iteration set "nearest_node" to actually created new_node
                nearest_node = new_node


class SchemaTree(Tree):
    """
    Tree for pyspark Schemas
    Inherited from Tree

    Attributes
    ----------
    self.data_type : Data type of the node
    self.nullable : Is the node nullable
    self.element_type : Element type of the node
    self.contains_null : When node has element type, if this can contain null

    Methods
    -------
    TODO: Add all methods
    
    add_path_to_tree()
        Adds a path (pigeonhole) to the tree
    """
    def __init__(self, name:str = 'root', data_type = None, nullable:bool = True, element_type:Optional[basic_spark_types] = None, contains_null:Optional[bool] = None, parent:Optional['Tree'] = None, children:Optional[List['Tree']] = None):
        # Call Constructor of super class
        super().__init__(name, parent, children)
        # alias needed for Flattening
        self.data_type = data_type
        self.nullable = nullable
        self.element_type = element_type
        self.contains_null = contains_null

    def __repr__(self):
        if self._name == "root":
            # Root has no data_type
            rep = self._name
        else:
            rep = f"{self._name} : {self.data_type}"
        return repr(rep)
    

    def _get_tree_as_list(self, tree_list:List = []) -> List[Tuple]:
        """
        Returns the tree as list with tuples. Every single node is one list entity.
        The tuples contain the path to the node, data type, nullable, element type and contains null.
        Mainly needed for comparing trees.

        Returns
        ----------
        list[tuple]
            List of tuples
        """
        # Attention: Here we are not working with copies of the list. We are working with one central list and handing over the pointers!
        if self._name == "root":
            # Ignore root node because its no "real" node
            tree_list = []
        else:
            tree_list.append((self.get_path_to_node("."), self.data_type, self.nullable, self.element_type, self.contains_null))
        for child in self._children:
            tree_list = child._get_tree_as_list(tree_list)
        return tree_list

    def add_struct_type_to_tree(self, struct:StructType, parents:List[str] = []) -> None:
        """
        Ingests a pyspark StructType as tree

        Parameters
        ----------
        struct : StructType
            A pyspark StructType (Schema)
        parents : list[str]
            A list with the ordered predecessor nodes
        """
        # Search the parent node when parents are not empty (first iteration)
        if self._name == "root":
            node = self
        else:
            node, missing_nodes = self.search_node_by_path(parents)
            assert len(missing_nodes) == 0, f"It seems that the StructType was not processed in a manner way. Missing nodes: {missing_nodes}"

        for field in struct:
            # Special case for arrays with elementType different to Field, Struct or Array
            if isinstance(field.dataType, ArrayType) and not isinstance(field.dataType.elementType, (StructType, StructField, ArrayType)):
                # Create a new node with element_type and contains_null
                new_node = SchemaTree(name = field.name, data_type = field.dataType.typeName(), nullable = field.nullable, element_type = field.dataType.elementType, contains_null = field.dataType.containsNull)
            else:
                # Create a new node without element_type and contains_null
                new_node = SchemaTree(name = field.name, data_type = field.dataType.typeName(), nullable = field.nullable)

            # Add me as parent of newly created child
            new_node.set_parent(node)

            # Add actual field to the list of parents and call the "sons"
            if isinstance(field.dataType, StructType):
                list_of_parent_nodes = parents.copy()
                list_of_parent_nodes.append(field.name)
                new_node.add_struct_type_to_tree(field.dataType, list_of_parent_nodes)
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, (StructType, StructField, ArrayType)):
                # Only for above defined types an named layer follows
                list_of_parent_nodes = parents.copy()
                list_of_parent_nodes.append(field.name)
                new_node.add_struct_type_to_tree(field.dataType.elementType, list_of_parent_nodes)
    
    def generate_fully_flattened_paths(self) -> json:
        """
        Generates a field-path list which can be used as starting point for flattening configuration.

        Returning
        ----------
        list: List with dicts to every leaf-path
        """
        leafs = self.get_leafs()
        fields = []
        for leaf in leafs:
            fields.append({"path": leaf.get_path_to_node("."),
                           "is_identifier": False,
                           "alias": None})
        # Embed List in dict and convert to json
        return json.dumps({"field-paths": fields})