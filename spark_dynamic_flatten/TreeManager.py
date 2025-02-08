from spark_dynamic_flatten.Tree import Tree, FlattenTree, SchemaTree
import os
import json
from pyspark.sql.types import StructType, ArrayType

class TreeManager(object):
    """
    TreeManager class is used create a Tree-object out of:
        - a pyspark schema file for example for easy comparison of schemas
        - a flattening configuration which is used to flatten a nested dataframe
    After the TreeManager has built the tree, the instance points to the root node of the created tree.
    Also have a look for methods coming from trees (root-node).
    Attributes
    ----------
        self.root : reference to root node of created tree
        self.source_table : source table name
        self.target_table : target table name

    Methods
    -------
    get_root_node()
        returns the root node of related tree
    print_tree()
        Prints the tree
    """
    def __init__(self, tree_class = Tree):
        """
        Parameters
        ----------
        tree_class : Tree
            The class-object of Tree or one of the inherited classes. Root-node will be instance of this class.
        source_table : str
            Name of the source table - additional information for flattening purposes
        target_table : str
            Name of the target table - additional information for flattening purposes
        """
        # Create root instance of 
        self.root = tree_class("root")

    def _add_path_to_tree(self, path:str, alias:str = None, is_identifier:bool = False) -> None:
        """
        Adds a path to tree and defines for leaf-node, if the leaf should be renmamed (aliased)
        and if it's an key-like value.

        The path should be a fully defined path (separated by dots) of an dataframe schema. Wildcards are actually not supported.
        E.g. path = "data.to.nested.leaf_field"

        Parameters
        ----------
        path : str
            Path to a nested field in a schema
        alias : str, optional
            Alias name for the nested field (Default = None)
        is_identifier : bool, optional
            should the nested field be handled as key-field (Default = False)
        """
        # Hand-over parameters to tree-specific function
        self.root.add_path_to_tree(path, alias, is_identifier)

    def print_tree(self) -> None:
        """
        Prints the tree
        """
        self.root.print_tree()

    def get_root_node(self) -> object:
        """Returns the root-node of the tree

        Returns
        ----------
        object
            Tree or one of the inherited classes of Tree
        """
        return self.root
    
    @staticmethod
    def from_struct_type(struct: StructType) -> "TreeManager":
        """
        Creates a tree based on a pyspark StructType

        Parameters
        ----------
        struct : StructType
            Pyspark schema of a Dataframe/Table
        """
        # Create an instance and initialize the root node
        tm = TreeManager(SchemaTree)
        # Get root node to start from there
        root = tm.get_root_node()
        # Call method to create tree based on StructType
        root.add_struct_type_to_tree(struct)
        return tm

    @staticmethod
    def from_schema_json_string(schema: str) -> "TreeManager":
        """
        Creates a tree based on a pyspark schema stored as Json string

        Parameters
        ----------
        schema : str
            Schema Json string
        """
        struct = StructType.fromJson(json.loads(schema))
        return TreeManager.from_struct_type(struct)
    
    @staticmethod
    def from_schema_json_file(file_path: str) -> "TreeManager":
        """
        Creates a tree based on a pyspark schema file

        Parameters
        ----------
        file_path : str
            Path to the pyspark schema file
        """
        assert file_path is not None
        # Open the file at the reference path and read its contents as a JSON string
        with open(file_path, "r") as f:
            # Parse the JSON string into a TreeManager and return it
            return TreeManager.from_schema_json_string(f.read())

    @staticmethod
    def from_flatten_json_dict(json_dict: dict) -> "TreeManager":
        """
        Creates a tree based on a configuration dict for purpose of flatten a nested pyspark Dataframe.


        Parameters
        ----------
        json_dict : dict
            Configuration dictionary with following structure {source_table, target_table, field_paths [{path, alias, in_identifier}]}
        """
        # Create instance of TreeManager for actually read file and create root of class FlattenTree
        tm = TreeManager(FlattenTree)

        # Add path to TreeManager instance and let's build the tree
        for entity in json_dict["field_paths"]:
            tm._add_path_to_tree(**entity)
        return tm

    @staticmethod
    def from_flatten_json_string(json_string: str) -> "TreeManager":
        """
        Creates a tree based on a configuration Json string for purpose of flatten a nested pyspark Dataframe.


        Parameters
        ----------
        json_string : str
            Json string with following structure {source_table, target_table, field_paths [{path, alias, in_identifier}]}
        """
        json_dict = json.loads(json_string)
        return TreeManager.from_flatten_json_dict(json_dict)

    @staticmethod
    def from_flatten_json_file(file_path: str) -> "TreeManager":
        """
        Creates a tree based on a Json file for purpose of flatten a nested pyspark Dataframe.


        Parameters
        ----------
        file_path : str
            Json file with following structure {source_table, target_table, field_paths [{path, alias, in_identifier}]}
        """
        assert file_path is not None
        if not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)
        # Open the file at the reference path and read its contents as a JSON string
        with open(file_path, "r") as f:
            # Parse the JSON string into a TreeManager and return it
            return TreeManager.from_flatten_json_string(f.read())