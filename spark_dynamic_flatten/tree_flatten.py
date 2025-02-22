"""Module providing a FlattenTree which is used as configuration for flattening a dataframe.
This tree inherits from the generic Tree implementation"""

from typing import List, Optional, Union
from spark_dynamic_flatten import Tree

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

        is_child_wildcard(self) -> bool:
            Checks if at least one child is a wildcard (child name="*")

        add_path_to_tree(self, path:str, alias:str = None, is_identifier:bool = False) -> None:
            Adds a path to the tree
    """

    def __init__(self,
                 name:str = 'root',
                 alias:str = None,
                 is_identifier:bool = False,
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

    def __repr__(self):
        if self._name == "root":
            # Root has no alias and is_identifier
            rep = self._name
        else:
            rep = f"{self._name} : {self._alias} - {self._is_identifier}"
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
        # Search if the complete path is already existing.
        # If not, we get back the last existing node and the missing part of path
        nearest_node, missing_path = self.search_node_by_path(path_list)
        if len(missing_path) > 0:
            for missing_node in missing_path:
                # Create new node
                if missing_node == missing_path[-1]:
                    # This is a leaf - so we have to add also the alias to the leaf
                    new_node = FlattenTree(missing_node, parent = nearest_node, alias = alias, is_identifier = is_identifier)
                    if missing_node == Tree.WILDCARD_CHAR:
                        # When name of leaf-node is Wildcard, this is a special case and details has to be inherited to parent
                        nearest_node.set_alias(alias)
                        nearest_node.set_is_identifier(is_identifier)
                else:
                    new_node = FlattenTree(missing_node, parent = nearest_node)
                nearest_node.add_child(new_node)
                # For next iteration set "nearest_node" to actually created new_node
                nearest_node = new_node
