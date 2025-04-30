"""Module providing a SchemaTree for handling of spark schemas.
This tree inherits from the generic Tree implementation"""

import os
import json
from typing import List, Tuple, Optional
from pyspark.sql.types import StructType, ArrayType, StructField, MapType
from spark_dynamic_flatten import Tree
from spark_dynamic_flatten.utils import BASIC_SPARK_TYPES, get_pyspark_sql_type

class SchemaTree(Tree):
    """
    A class to represent a tree for PySpark dataframe schema.

    This class provides functionalities to construct, manipulate, and compare
    hierarchical schema structures for PySpark dataframes.

    This tree inherits from the generic Tree implementation.

    Methods:
        __init__(self, name:str = 'root', data_type = None, nullable:bool = True, metadata:dict = None, element_type:Optional[BASIC_SPARK_TYPES] = None, contains_null:Optional[bool] = None, key_type:Optional[BASIC_SPARK_TYPES] = None, value_type:Optional[BASIC_SPARK_TYPES] = None, parent:Optional['Tree'] = None, children:Optional[List['Tree']] = None):
            Initializes a SchemaTree instance.

        __repr__(self) -> str:
            Returns a string representation of the SchemaTree.

        get_data_type(self) -> Optional[BASIC_SPARK_TYPES]:
            Retrieves the data type of the current SchemaTree node.

        get_metadata(self) -> dict:
            Retrieves the metadata of the current SchemaTree node.

        get_element_type(self) -> Optional[BASIC_SPARK_TYPES]:
            Retrieves the element type if the current SchemaTree node is an array.

        get_contains_null(self) -> Optional[bool]:
            Checks if the array elements can contain null values.

        get_key_type(self) -> Optional[BASIC_SPARK_TYPES]:
            Retrieves the key type if the current SchemaTree node is a map.

        get_value_type(self) -> Optional[BASIC_SPARK_TYPES]:
            Retrieves the value type if the current SchemaTree node is a map.

        add_struct_type_to_tree(self, struct:StructType, parents:List[str] = None) -> None:
            Creates/adds a StructType to the tree

        add_path_to_tree(self, path:str, data_type:str, nullable:bool = True, metadata:dict = None, element_type:str = None, contains_null:bool = None, key_type:str = None, value_type:str = None) -> None:
            Adds a path separated by "." to tree  

        generate_fully_flattened_paths(self) -> dict:
            Flattens the tree to it's leaf-nodes and returns leafs as dict

        generate_fully_flattened_json(self) -> json:
            Flattens the tree to it's leaf-nodes and returns leafs as json string

        save_fully_flattened_json(self, path, file_name):
            Flattens the tree to it's leaf-nodes and writes it as json file.

        generate_fully_flattened_struct(self) -> StructType:
            Flattens the tree and returns it as spark schema StuctType

        subtract(self, other: 'SchemaTree') -> set:
            Subtracts another SchemaTree from this SchemaTree and returns the difference as a set of tuples.

        intersection(self, other: 'SchemaTree') -> SchemaTree:
            Returns the intersections of two SchemaTrees as a new SchemaTree. 

        symmetric_difference(self, other: 'SchemaTree') -> set:
            Searches for symmetric differences of two SchemaTrees and returns it as a set of tuples. 
    """

    def __init__(self,
                 name:str = 'root',
                 data_type = None,
                 nullable:bool = True,
                 metadata:dict = None,
                 element_type:Optional[BASIC_SPARK_TYPES] = None,
                 contains_null:Optional[bool] = None,
                 key_type:Optional[BASIC_SPARK_TYPES] = None,
                 value_type:Optional[BASIC_SPARK_TYPES] = None,
                 parent:Optional['Tree'] = None,
                 children:Optional[List['Tree']] = None
                ):
        # Call Constructor of super class
        super().__init__(name, parent, children)
        # alias needed for Flattening
        self.data_type = data_type
        self.nullable = nullable
        self.element_type = element_type
        self.contains_null = contains_null
        self.metadata = metadata
        self.key_type = key_type
        self.value_type = value_type

    def __repr__(self):
        if self._name == "root":
            # Root has no data_type
            rep = self._name
        else:
            rep = f"{self._name} : {self.data_type}"
            if self.nullable is not None:
                rep = rep + f" (nullable = {self.nullable})"
            if self.element_type is not None:
                rep = rep + f" (element_type = {self.element_type})"
            if self.contains_null is not None:
                rep = rep + f" (contains_null = {self.contains_null})"
        return repr(rep)

    def get_data_type(self):
        """
        Returns the data type of the node
        """
        return self.data_type

    def get_nullable(self) -> bool:
        """
        Returns the nullable setting of the node
        """
        return self.nullable
    
    def get_metadata(self) -> dict:
        """
        Returns the metadata setting of the node
        """
        return self.metadata

    def get_element_type(self):
        """
        Returns the element_type of the node
        """
        return self.element_type

    def get_contains_null(self):
        """
        Returns the contains_null setting of element type of the node
        """
        return self.contains_null

    def get_key_type(self):
        """
        Returns the key type of the node
        """
        return self.key_type

    def get_value_type(self):
        """
        Returns the value type of the node
        """
        return self.value_type

    def _get_tree_as_list(self, node:"Tree", tree_list:List = None) -> List[Tuple]:
        """
        Returns the tree as list with tuples. Every single node is one list entity.
        The tuples contain the path to the node, data type, nullable, element type and contains null.
        Mainly needed for comparing trees.
        But be aware, that for SchemaTrees mybe existing matadata is not included in the list and
        also for comparing SchemaTrees!

        Returns
        ----------
        list[tuple]
            List of tuples
        """
        # Attention: Here we are not working with copies of the list.
        # We are working with one central list and handing over the pointers!
        if node.get_name() == "root":
            # Ignore root node because its no "real" node
            tree_list = []
        else:
            tree_list.append((node.get_path_to_node("."), node.get_data_type(), node.get_nullable(), node.get_element_type(), node.get_contains_null()))
        for child in node.get_children():
            tree_list = self._get_tree_as_list(child, tree_list)
        return tree_list

    def add_struct_type_to_tree(self, struct:StructType, parents:List[str] = None) -> None:
        """
        Ingests a pyspark StructType as tree

        Parameters
        ----------
        struct : StructType
            A pyspark StructType (Schema)
        parents : list[str]
            A list with the ordered predecessor nodes
        """
        # Make sure to start from root-node. When parents are None -> first iteration
        if not parents:
            assert self._name == "root", "This method has to be called on root-node instance!"

        if self._name == "root":
            node = self
        else:
            node, missing_nodes = self.search_node_by_path(parents)
            assert len(missing_nodes) == 0, f"It seems that the StructType was not processed in a manner way. Missing nodes: {missing_nodes}"

        for field in struct:
            # Special case for arrays with elementType different to Field, Struct or Array
            if isinstance(field.dataType, ArrayType): # and not isinstance(field.dataType.elementType, (StructType, StructField, ArrayType)):
                # Create a new node with element_type and contains_null
                new_node = SchemaTree(name = field.name, data_type = field.dataType.typeName(), nullable = field.nullable, metadata = field.metadata, element_type = field.dataType.elementType.typeName(), contains_null = field.dataType.containsNull)
            elif isinstance(field.dataType, MapType):
                # Create a new node with element_type and contains_null
                new_node = SchemaTree(name = field.name, data_type = field.dataType.typeName(), nullable = field.nullable, metadata = field.metadata, key_type = field.dataType.keyType.typeName(), value_type = field.dataType.valueType.typeName())
            else:
                # Create a new node without element_type and contains_null
                new_node = SchemaTree(name = field.name, data_type = field.dataType.typeName(), nullable = field.nullable, metadata = field.metadata)

            # Add me as parent of newly created child
            new_node.set_parent(node)

            # Add actual field to the list of parents and call the "sons"
            if isinstance(field.dataType, StructType):
                if parents is None:
                    list_of_parent_nodes = []
                else:
                    list_of_parent_nodes = parents.copy()
                list_of_parent_nodes.append(field.name)
                new_node.add_struct_type_to_tree(field.dataType, list_of_parent_nodes)
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, (StructType, StructField, ArrayType)):
                # Only for above defined types an named layer follows
                if parents is None:
                    list_of_parent_nodes = []
                else:
                    list_of_parent_nodes = parents.copy()
                list_of_parent_nodes.append(field.name)
                new_node.add_struct_type_to_tree(field.dataType.elementType, list_of_parent_nodes)

    def add_path_to_tree(self, path:str, data_type:str, nullable:bool = True, metadata:dict = None, element_type:str = None, contains_null:bool = None, key_type:str = None, value_type:str = None) -> None:
        """
        Adds a path (pigeonhole) to the tree. Overwrite method of super class.

        Parameters
        ----------
        path : str
            Path to be pigeonholed to the tree
        data_type : str
            DataType of the node
        nullable : bool
            should the node be nullable
        metadata : dict
            metadata
        element_type : str
        contains_null : bool
        key_type : str
        value_type : str

        """
        # Split path
        path_list = path.split(".")
        # Search if the complete path is already existing.
        # For SchemaTree you have to make sure that the parents of the nodepath are already in the tree.
        # It makes no sense for SchemaTree to generate parents based on a higher level node.
        # Therefore the datatype and nullable could not be taken from a higher level.
        # So at least there should only last node missing. Otherwise it's an error
        nearest_node, missing_path = self.search_node_by_path(path_list)
        if len(missing_path) == 1:
            for missing_node in missing_path:
                # Create new node
                new_node = SchemaTree(missing_node,
                                        parent = nearest_node,
                                        data_type = data_type,
                                        nullable = nullable,
                                        metadata = metadata,
                                        element_type = element_type,
                                        contains_null = contains_null,
                                        key_type = key_type,
                                        value_type = value_type
                                        )
                nearest_node.add_child(new_node)
                # For next iteration set "nearest_node" to actually created new_node
                nearest_node = new_node
        elif len(missing_path) > 1:
            # This node seems to be hanging in the air and is not considered in resulting Tree
            # Maybe this should be an exception?
            print(f"WARNING: Following path could not be added to tree, because of missing parent: {path}")
            pass

    def generate_fully_flattened_paths(self) -> dict:
        """
        Generates a field-path list which can be used as starting point for flattening configuration.

        Returning
        ----------
        dict: Dictionary with every leaf-path can be used for flatten logic
        """
        leafs = self.get_leafs()
        fields = []
        dict_fieldnames = {}
        for leaf in leafs:
            # When same name of leaf nodes exist multiple times in tree, we increment and add an alias
            alias = None
            if leaf.get_name() in dict_fieldnames:
                n = dict_fieldnames[leaf.get_name()]
                n = n + 1
                dict_fieldnames[leaf.get_name()] = n
                alias = f"{leaf.get_name()}#{n}"
            else:
                dict_fieldnames[leaf.get_name()] = 1

            if leaf.get_element_type():
                # When leaf is array (only arrays can have element types), we have to append
                # a ".*" so that this array will also be exploded in flattening
                fields.append({"path": f"{leaf.get_path_to_node('.')}.{Tree.WILDCARD_CHAR}",
                           "is_identifier": False,
                           "alias": alias})
            else:
                fields.append({"path": leaf.get_path_to_node("."),
                            "is_identifier": False,
                            "alias": alias})
        # Embed List in dict-key field-paths which is entry point for creating a TreeFlatten
        return {"field_paths": fields}

    def generate_fully_flattened_json(self) -> json:
        """
        Generates a field-path list which can be used as starting point for flattening configuration.

        Returning
        ----------
        Json: Json-String with every leaf-path can be used for flatten logic
        """
        return json.dumps(self.generate_fully_flattened_paths())

    def save_fully_flattened_json(self, path, file_name):
        """
        Saves a json file with configuration to fully flatten the tree.
        This file can directly be used for flattening (if the schema should be fully flattened).

        Hint:
        When a leaf-name (leaf-nodes) is not unique you have to take care to define aliases, to be unique on leaf level!
        """
        if os.path.isabs(path):
            file_path = os.path.join(path, file_name)
        else:
            file_path = os.path.join(os.path.abspath(path), file_name)

        raw_file_path = r"{}".format(file_path)  # pylint: disable=C0209

        json_str = self.generate_fully_flattened_json()

        with open(raw_file_path, "w", encoding="utf-8") as file:
            file.write(json_str)
        print(f"File {file_path} was sucessfully written.")

    def generate_fully_flattened_struct(self) -> StructType:
        """
        Generates a spark StrucType which only contains leaf-nodes of tree.
        Can be used as schema for a fully flattened dataframe

        Returning
        ----------
        StructType: Spark schema with every leaf-path
        """
        leafs = self.get_leafs()
        fields = []
        dict_fieldnames = {}
        for leaf in leafs:
            # When same name of leaf nodes exist multiple times in tree, we increment and add an alias
            new_name = None
            if leaf.get_name() in dict_fieldnames:
                n = dict_fieldnames[leaf.get_name()]
                n = n + 1
                dict_fieldnames[leaf.get_name()] = n
                new_name = f"{leaf.get_name()}#{n}"
            else:
                new_name = leaf.get_name()
                dict_fieldnames[leaf.get_name()] = 1

            if leaf.get_data_type() == "array":
                # When leaf is an array, the element type of array is used
                fields.append(StructField(new_name, ArrayType(get_pyspark_sql_type(leaf.get_element_type()), leaf.get_contains_null()), leaf.get_nullable(), leaf.get_metadata()))
            elif leaf.get_data_type() == "map":
                # When leaf is an array, the element type of array is used
                fields.append(StructField(new_name, MapType(get_pyspark_sql_type(leaf.get_key_type()), get_pyspark_sql_type(leaf.get_value_type())), leaf.get_nullable(), leaf.get_metadata()))
            else:
                fields.append(StructField(new_name, get_pyspark_sql_type(leaf.get_data_type()), leaf.get_nullable(), leaf.get_metadata()))
        # Embed List in dict-key field-paths which is entry point for creating a TreeFlatten
        return StructType(fields)

    def _tree_to_tuples(self, node: 'SchemaTree', case_sensitive: bool = True) -> List[Tuple]:
        """
        Converts a SchemaTree to a list of tuples representing the tree structure.

        Parameters
        ----------
        node : SchemaTree
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

            tuples = [(path, node.data_type, node.nullable, node.element_type, node.contains_null, node.key_type, node.value_type)]
        
        for child in node.get_children():
            tuples.extend(self._tree_to_tuples(child))
        return tuples

    def _tuples_to_dict(self, tuples: set) -> List[dict]:
        return [{"path": x[0], "data_type": x[1], "nullable": x[2], "element_type": x[3], "contains_null": x[4], "key_type": x[5], "value_type": x[6]} for x in tuples]

    def _tuples_to_tree(self, tuples: List[Tuple]) -> Optional['SchemaTree']:
        """
        Converts a list of tuples back to a SchemaTree.

        Parameters
        ----------
        tuples : List[Tuple]
            A list of tuples representing the tree structure.

        Returns
        -------
        SchemaTree
            The reconstructed SchemaTree.
        """
        if not tuples:
            return None

        # Create a root node
        root = SchemaTree("root")

        # sort the tuples based on the level of the nodes. E.g a node with name/path node1.node11 is on level 2 whereas node1 is a level 1 node
        sorted_tuples = sorted(tuples, key=lambda x: len(x[0]))

        # Add child nodes
        for path, data_type, nullable, element_type, contains_null, key_type, value_type in sorted_tuples:
            root.add_path_to_tree(path = path,
                                   data_type = data_type,
                                   nullable = nullable,
                                   metadata = {},
                                   element_type = element_type,
                                   contains_null = contains_null,
                                   key_type = key_type,
                                   value_type = value_type
                                    )
        if root.equals(SchemaTree("root")):
            pass
        else:
            return root

    def to_struct_type(self) -> StructType:
        """
        Converts the SchemaTree back to a PySpark StructType.

        Returns
        -------
        StructType
            The PySpark StructType representing the schema.
        """
        fields = []
        for child in self.get_children():
            fields.append(self._node_to_struct_field(child))
        return StructType(fields)

    def _node_to_struct_field(self, node: 'SchemaTree') -> StructField:
        """
        Helper method to convert a SchemaTree node to a StructField.

        Parameters
        ----------
        node : SchemaTree
            The node to convert.

        Returns
        -------
        StructField
            The corresponding StructField.
        """
        if node.get_data_type() == "struct":
            # Recursively convert child nodes
            child_fields = [self._node_to_struct_field(child) for child in node.get_children()]
            data_type = StructType(child_fields)
        elif node.get_data_type() == "array":
            # When its array, it could be an array with a Sruct or a array with a simple data-type
            if node.get_element_type() == "struct":
                # Array with Struct
                child_fields = [self._node_to_struct_field(child) for child in node.get_children()]
                data_type = ArrayType(StructType(child_fields), node.get_contains_null())
            else:
                # Array with simple datatype
                element_type = get_pyspark_sql_type(node.get_element_type())
                data_type = ArrayType(element_type, node.get_contains_null())
        elif node.get_data_type() == "map":
            key_type = get_pyspark_sql_type(node.get_key_type())
            value_type = get_pyspark_sql_type(node.get_value_type())
            data_type = MapType(key_type, value_type)
        else:
            data_type = get_pyspark_sql_type(node.get_data_type())

        return StructField(node.get_name(), data_type, node.get_nullable(), node.get_metadata())
