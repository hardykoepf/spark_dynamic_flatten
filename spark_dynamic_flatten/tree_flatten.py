"""Module providing a FlattenTree which is used as configuration for flattening a dataframe.
This tree inherits from the generic Tree implementation"""

from typing import List, Optional, Union, Tuple
from pyspark.sql.types import StructType
from spark_dynamic_flatten import Tree, SchemaTree

class FlattenTree(Tree):
    """
    Module providing a FlattenTree which is used as configuration for flattening a dataframe.
    This tree inherits from the generic Tree implementation.

    This class provides functionalities to configure, manipulate, and apply
    flattening operations on hierarchical dataframe structures.

    This tree inherits from the generic Tree implementation.

    Methods:
        __init__(self, name: str = 'root', parent: Optional['Tree'] = None, children: Optional[List['Tree']] = None):
            Initializes a FlattenTree instance.
        
        __repr__(self) -> str:
            Returns a string representation of the FlattenTree.

        set_alias(self, alias: str) -> None:
            Sets an alias for the current FlattenTree node.

        get_alias(self) -> Optional[str]:
            Retrieves the alias of the current FlattenTree node.

        set_is_identifier(self, is_identifier:bool) -> None:
            Sets the is_identifier for the current FlattenTree node.

        get_is_identifier(self) -> bool:
            Retrieves the is_identifier of the current FlattenTree node.

        set_explode_with_pos(self, explode_with_pos:bool) -> None:
            Sets the explode with_pos for the current FlattenTree node.

        get_explode_with_pos(self) -> bool:
            Retrieves the explode_With_pos of the current FlattenTree node.

        is_child_wildcard(self) -> bool:
            Checks if at least one child is a wildcard (child name="*")

        add_path_to_tree(self, path:str, alias:str = None, is_identifier:bool = False) -> None:
            Adds a path to the tree

        subtract(self, other: 'FlattenTree') -> 'FlattenTree':
            Subtracts another FlattenTree from this FlattenTree and returns the difference as a new FlattenTree.    
    """

    def __init__(self,
                 name:str = 'root',
                 alias:str = None,
                 is_identifier:bool = False,
                 explode_with_pos:bool = False,
                 parent:Optional['Tree'] = None,
                 children:Optional[List['Tree']] = None
                ):
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
        self._alias = alias
        # is_identifier needed for Flattening
        self._is_identifier = is_identifier
        # explode_with_pos needed for Flattening
        self._explode_with_pos = explode_with_pos

    def __repr__(self):
        rep = f"{self._name}"
        if self._alias is not None:
            rep = rep + f" (alias = {self._alias})"
        if self._is_identifier:
            rep = rep + f" (is_identifier = {self._is_identifier})"
        if self._explode_with_pos:
            rep = rep + f" (explode_with_pos = {self._explode_with_pos})"
        return repr(rep)

    def set_alias(self, alias:str) -> None:
        """
        Set the alias of the node

        Parameter
        ----------
        alias : Alias of the node
        """
        self._alias = alias

    def get_alias(self) -> Union[str, None]:
        """
        Returns the alias of the node

        Returning
        ----------
        Union[str, None] : Alias of the node
        """
        return self._alias

    def set_is_identifier(self, is_identifier:bool) -> None:
        """
        Set True if node is identifier. Otherwise False

        Parameter
        ----------
        is_identifier : Is the node identifier
        """
        self._is_identifier = is_identifier

    def get_is_identifier(self) -> bool:
        """
        Returns True if node is identifier. Otherwise False

        Returning
        ----------
        bool : Is the node identifier
        """
        return self._is_identifier
    
    def set_explode_with_pos(self, explode_with_pos:bool) -> None:
        """
        Set to True if node needs to be exploded with additional position column. Otherwise False
        
        Parameter
        ---------
        explode_with_pos : Needs the node (has to be an array/map) to be exploded with position column
        """
        self._explode_with_pos = explode_with_pos

    def get_explode_with_pos(self) -> bool:
        """
        Returns the explode_with_pos attribute of the node.

        Returning
        ---------
        bool : Is the node to be exploded with position
        """
        return self._explode_with_pos


    def is_child_wildcard(self) -> bool:
        """
        Checks if the (at least one) child of the node is a wildcard
        (node-name= Wildcard)

        Returning
        ----------
        bool
        """
        for child in self._children:
            if child.get_name() == Tree.WILDCARD_CHAR:
                return True
        return False

    def add_path_to_tree(self, path:str, alias:str = None, is_identifier:bool = False, explode_with_pos:bool = False) -> None:
        """
        Adds a path (pigeonhole) to the tree. Overwrite method of super class.
        This node also takes care about having alias and is_identifier on leaf nodes

        Parameters
        ----------
        path : str
            Path to be pigeonholed to the tree
        alias : str
            Alias name for this path/field
        is_identifier : bool
            Is the path/field key to target table
        explode_with_pos : bool
            Explode the path with position
        """
        # Sanity check: When explode_with_pos is set to True, also an alias has to be provided
        if explode_with_pos and alias is None:
            raise ValueError(f"{path}: When 'explode_with_pos' is set to True, also an alias has to be provided.")
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
                    new_node = FlattenTree(missing_node, parent = nearest_node, alias = alias, is_identifier = is_identifier, explode_with_pos = explode_with_pos)
                    if missing_node == Tree.WILDCARD_CHAR:
                        # When name of leaf-node is Wildcard, this is a special case and details has to be inherited to parent
                        nearest_node.set_alias(alias)
                        nearest_node.set_is_identifier(is_identifier)
                else:
                    new_node = FlattenTree(missing_node, parent = nearest_node)
                nearest_node.add_child(new_node)
                # For next iteration set "nearest_node" to actually created new_node
                nearest_node = new_node
        else:
            # Set attributes to already existing node/path. At least needed for array which should be exploded with position.
            # Therefore maybe the node in the middle was already created by another leaf in configuration.
            nearest_node.set_alias(alias)
            nearest_node.set_is_identifier(is_identifier)
            nearest_node.set_explode_with_pos(explode_with_pos)

    def _tree_to_tuples(self, node: 'FlattenTree', case_sensitive: bool = True) -> List[Tuple]:
        """
        Converts a FlattenTree to a list of tuples representing the tree structure.

        Parameters
        ----------
        node : FlattenTree
            The root-node of tree to convert.
        case_sensitive: bool
            Should the path for comparison be transfered to lower-case

        Returns
        -------
        List[Tuple]
            A list of tuples representing the tree structure.
        """
        if node.get_name() == "root":
            tuples = []
        else:
            path = node.get_path_to_node(".")
            if not case_sensitive:
                path = path.lower()

            tuples = [(path, node.get_alias(), node.get_is_identifier(), node.get_explode_with_pos())]
        
        for child in node.get_children():
            tuples.extend(self._tree_to_tuples(child))
        return tuples

    def _tuples_to_dict(self, tuples: set) -> List[dict]:
        return [{"path": x[0], "alias": x[1], "is_identifier": x[2], "explode_with_pos": x[3]} for x in tuples]

    def _tuples_to_tree(self, tuples: List[Tuple]) -> 'FlattenTree':
        """
        Converts a list of tuples back to a FlattenTree.

        Parameters
        ----------
        tuples : List[Tuple]
            A list of tuples representing the tree structure.

        Returns
        -------
        FlattenTree
            The reconstructed FlattenTree.
        """
        if not tuples:
            return None

        # Create a root node
        root = FlattenTree("root")

        # sort the tuples based on the level of the nodes. E.g a node with name/path node1.node11 is on level 2 whereas node1 is a level 1 node
        sorted_tuples = sorted(tuples, key=lambda x: len(x[0]))

        # Add child nodes
        for path, alias, is_identifier, explode_with_pos in sorted_tuples:
            root.add_path_to_tree(path = path,
                                   alias = alias,
                                   is_identifier = is_identifier,
                                   explode_with_pos = explode_with_pos
                                    )

        if root.equals(FlattenTree("root")):
            pass
        else:
            return root

    def generate_flattened_schema(self, nested_schema: StructType) -> StructType:
        """
        Generates a Schema (StructType) based on the FlattenTree.
        As Parameter the corresponding deeply nested StructType which should be flattened
        with this FlattenTree is needed.

        Parameters
        ----------
        nested_schema : StructType
            Deeply nested schema which should be flatten with this FlattenTree

        Returns
        -------
        StructType
            Schema which Dataframe should look like after flattening
        """
        # Make sure to start from root node of tree
        if self.is_root():
            root_node = self
        else:
            root_node = self

        # Generate a SchemaTree from supplied StructType
        struct_tree = SchemaTree("root")
        struct_tree.add_struct_type_to_tree(nested_schema)

        # Instanciate new SchemaTree
        result_tree = SchemaTree("root")

        for node in root_node.walk_tree():
            if node.is_leaf():
                # When its leaf, this will be a column after flatten
                path_to_leaf = node.get_path_to_node(".")
                # search counterpart in StructType to get details
                struct_node, missing_part = struct_tree.search_node_by_path(path_to_leaf)
                if missing_part:
                    # When the node doesn't match in complete, the StructType semms to not match to FlattenTree
                    # This is an error
                    raise TypeError('Supplied StrutType doesnt fit to Tree. Path could not be found: {path_to_leaf}')
                result_node = SchemaTree(
                    name=node.get_alias() if node.get_alias() else node.get_name(),
                    data_type=struct_node.get_data_type(),
                    nullable=struct_node.get_nullable(),
                    metadata={},
                    element_type=struct_node.get_element_type(),
                    contains_null=struct_node.get_contains_null(),
                    key_type=struct_node.get_key_type(),
                    value_type=struct_node.get_value_type(),
                    parent=result_tree
                )
                result_tree.add_child(result_node)
            elif node.get_explode_with_pos():
                # When an array has to be exploded with position, the position column has to be also part of the structure
                result_node = SchemaTree(
                    name=node.get_alias() if node.get_alias() else node.get_name(),
                    data_type=struct_node.IntegerType(),
                    nullable=True,
                    metadata={},
                    element_type=struct_node.StructField,
                    parent=result_tree
                )
                result_tree.add_child(result_node)
        result = result_tree.to_struct_type()
        return result
