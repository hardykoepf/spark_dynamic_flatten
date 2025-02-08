# pyspark_dataframe_tools

Tools to dynamically flatten and compare pyspark dataframes.

## Description

This project provides tools for working with PySpark dataframes, including functionality to dynamically flatten nested data structures and compare schemas. It is designed to help users manage complex data transformations and schema validations in PySpark.

## Features

- Dynamically flatten nested PySpark dataframes based on configuration.
- Compare schemas of different PySpark dataframes.
- Utility functions for schema manipulation and validation.

## Installation

To install the dependencies for this project, you can use [Poetry](https://python-poetry.org/). Ensure you have Python 3.8 or higher installed.

1. Clone the repository:
   ```sh
   git clone https://github.com/hardykoepf/pyspark_dataframe_tools.git
   cd pyspark_dataframe_tools

## Classes within this solution

The solution consists of three classes implementing specific trees:
- Tree: Basic tree class implementing standard tree data stucture with nodes referencing to the parent node and to the children nodes
- SchemaTree: Inherited from Tree. Especially for handling schemas of (pyspark) spark dataframes. With this class you can for example generate a json file on which flattening will happen.
- FlattenTree: Inherited from Tree. Especially for flattening a nested schema of spark dataframe

Also besides the trees, a TreeManager offers methods for creating a tree based on json file, json string or spark schema.

## Usage

Because two different use cases are implemented, we have to separate. The use cases are related to each other.

### Schemas

For importing a schema to a Tree structure, you have the option to use a Json file representing a spark schema, or you can import a json string representing a schema or at least already using a StrucType.
In general, when creating a Tree the TreeManager comes into play. The TreeManager offers methods for generating a Tree.
Especially for schemas, following static methods are offered (creating a Tree of SchemaTree instance nodes):
- TreeManager.from_struct_type(struct) -> TreeManager
- TreeManager.from_schema_json_string(json_str) -> TreeManager
- TreeManager.from_schema_json_file(json_file) -> TreeManager

#### Comparing Schemas
A helpful function comes with SchemaTree is to compare schemas of two PySpark dataframes, you can use the equals function.

```
from spark_dynamic_flatten.tree_manager import TreeManager

tree_schema1 = TreeManager.from_struct_type(df1.schema).get_root_node()
tree_schema2 = TreeManager.from_struct_type(df2.schema).get_root_node()

equals, differences = tree_schema1.equals(tree_schema2)
```

#### Generate configuration for fully flattening a dataframe

After parsing the schema of dataframe to a tree object, we generate the json config we can use for completely flatten the dataframe (or modify if we only need specific fields flattened).

```
from spark_dynamic_flatten.tree_manager import TreeManager

tree_schema1 = TreeManager.from_struct_type(df1.schema).get_root_node()
json_string = tree_schema1.generate_fully_flattened_json()
```

### Flatten
 
The configuration for flatten a nested structure is defined by the path to the leaf fields separated by a dot.
E.g. node1.node2.node3.leaf_field
For every path/field a alias and also the option if the field should be an identifier (key) for the flattened table.
To summarize, for every path/field to be flattened, a dictionary with following keys has to be defined:
- path
- alias
- is_identifier

E.g.:
{"path": "node1.node2.node3.leaf_field", "alias": "leaf_alias", "is_identifier": False}

At least, the paths are collected by an outer dict with the key "field_paths"
E.g.:
```
{ "field_paths": [
    {"path": "node1.node2.node3.leaf_field", "alias": "leaf_alias", "is_identifier": False},
    {"path": "node11.node22.node33.leaf_field2", "alias": None, "is_identifier": False}
    ]
}
```

To import the configuration, you have the option to have it as json file, json string or within a dict. Therefore again the TreeManager is used.
- TreeManager.from_flatten_type(struct) -> TreeManager
- TreeManager.from_flatten_json_string(json_str) -> TreeManager
- TreeManager.from_flatten_json_file(json_file) -> TreeManager

```
from spark_dynamic_flatten.tree_manager import TreeManager
from spark_dynamic_flatten.tree import FlattenTree
from spark_dynamic_flatten.flatten import Flatten

flatten = TreeManager.from_flatten_json_string(json_string).get_root()
df_flattened = Flatten.flatten(df1, flatten)

```