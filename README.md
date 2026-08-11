# spark_dynamic_flatten

Tools to dynamically flatten nested schemas with spark based on configuration and compare pyspark dataframe schemas.

## Description

This project provides tools for working with (Py)Spark dataframes, including functionality to dynamically flatten nested data structures and compare schemas. It is designed to help users manage complex data transformations and schema validations in PySpark.

## Features

- Dynamically flatten nested PySpark dataframes based on configuration (only flatten what is needed).
- Track array element positions during flattening using the `explode_with_pos` configuration.
- Compare schemas of different PySpark dataframes.
- Utility functions for schema manipulation and validation.

## Installation

To install the dependencies for this project, you can use [Poetry](https://python-poetry.org/). Ensure you have Python 3.8 or higher installed.

1. Clone the repository:
   ```sh
   git clone https://github.com/hardykoepf/spark_dynamic_flatten.git
   cd spark_dynamic_flatten
   ```

2. Alternatively, install it from PyPI:
   ```sh
   pip install pyspark-dynamic-flatten
   ```

## Classes within this solution

The solution consists of three classes implementing specific trees:
- **Tree**: Basic tree class implementing a standard tree data structure with nodes referencing the parent node and the children nodes.
- **SchemaTree**: Inherited from `Tree`. Specifically for handling schemas of (PySpark) dataframes. With this class, you can, for example, generate a JSON config file for the `Flatten` class based on a dataframe schema.
- **FlattenTree**: Inherited from `Tree`. Specifically for flattening a nested schema of a Spark dataframe.

Additionally, a `TreeManager` offers methods for creating a tree based on a JSON file, JSON string, or Spark schema.  

The flattening is executed within the `Flatten` class.

### General Tree Functions

The trees are defined to be self-managed. This means there is no separation between a tree and node in implementation.  

To get a quick overview of how the tree looks, the `print()` method prints the tree:
```python
root_node_of_tree.print()
```

If you need the tree as a list, the `get_tree_as_list()` function will return a list with the complete path to every node:
```python
root_node_of_tree.get_tree_as_list()
```

The `get_tree_layered()` method will return the tree as a nested list. Each layer is represented by a separate list, and the outer list contains all the layers:
```python
root_node_of_tree.get_tree_layered()
```

### Comparing Trees

Comparing trees is helpful for identifying differences between schemas. Use the `equals` function to check if two trees are equal:
```python
from spark_dynamic_flatten import TreeManager

tree_schema1 = TreeManager.from_struct_type(df1.schema)
tree_schema2 = TreeManager.from_struct_type(df2.schema)

if tree_schema1.equals(tree_schema2):
    print("Schemas are equal")
```

Other tree comparison functions include:
- **Symmetric Difference**: Use `symmetric_difference` to find differences in both directions.
- **Subtraction**: Use `subtract` to find paths in one tree that are not in another.
- **Intersection**: Use `intersection` to find the common parts of two trees.

### Flattening Dataframes

The `Flatten` class provides the main functionality for flattening nested dataframes. The configuration defines the paths to be flattened, aliases, and whether fields are identifiers.

#### New Feature: Track Array Element Positions

The `explode_with_pos` configuration allows you to track the position of array elements during flattening. When this option is enabled for a specific field, the position of each array element will be included in the flattened dataframe as a separate column.

##### Configuration for `explode_with_pos`

To enable this feature, set the `explode_with_pos` attribute to `True` for the relevant field in the configuration. Additionally, you must define an alias for the position column.

Example configuration:
```json
{
  "field_paths": [
    {
      "path": "node1.node2.array_field",
      "alias": "array_value",
      "is_identifier": False,
      "explode_with_pos": True
    }
  ]
}
```

In this example:
- `path`: The path to the array field to be exploded.
- `alias`: The alias for the array values.
- `is_identifier`: Whether this field is an identifier.
- `explode_with_pos`: Enables tracking of array element positions.

The position column will be named using the alias defined in the configuration. For example, if the alias is `array_value`, the position column will be named `node1.node2.array_field#array_value`.

##### Example Usage

```python
from spark_dynamic_flatten import TreeManager, Flatten

# Load the configuration
json_config = """
{
  "field_paths": [
    {
      "path": "node1.node2.array_field",
      "alias": "array_value",
      "is_identifier": False,
      "explode_with_pos": True
    }
  ]
}
"""

# Create a FlattenTree from the configuration
root_tree = TreeManager.from_flatten_json_string(json_config)

# Flatten the dataframe
df_flattened = Flatten.flatten(df, root_tree)

# The resulting dataframe will include a position column for the array elements
df_flattened.show()
```

#### General Flatten Configuration

The configuration for flattening a nested structure is defined by the path to the leaf fields separated by a dot.  
E.g. `node1.node2.node3.leaf_field`  
For every path/field, an alias and a boolean indicating if the field should be an identifier (key) for the flattened table are defined.  

To summarize, for every path/field to be flattened, a dictionary with the following keys has to be defined:
- `path`
- `alias`
- `is_identifier`

Example:
```json
{"path": "node1.node2.node3.leaf_field", "alias": "leaf_alias", "is_identifier": False}
```

At least, the paths are collected by an outer dictionary with the key `field_paths`:
```json
{
  "field_paths": [
    {"path": "node1.node2.node3.leaf_field", "alias": "leaf_alias", "is_identifier": False},
    {"path": "node11.node22.node33.leaf_field2", "alias": null, "is_identifier": False}
  ]
}
```

This JSON configuration can be generated based on a dataframe schema. See the example above using the `generate_fully_flattened_json` method based on a `SchemaTree`.  

To import the configuration, you have the option to have it as a JSON file, JSON string, or within a dictionary. Use the `TreeManager` for this:
- `TreeManager.from_flatten_type(struct) -> FlattenTree`
- `TreeManager.from_flatten_json_string(json_str) -> FlattenTree`
- `TreeManager.from_flatten_json_file(json_file) -> FlattenTree`

When a `FlattenTree` is instantiated by the configuration, use this instance together with the dataframe to be flattened and call the `flatten` method of the `Flatten` class:
```python
from spark_dynamic_flatten import TreeManager, FlattenTree, Flatten

root_tree = TreeManager.from_flatten_json_string(json_string)
df_flattened = Flatten.flatten(df1, root_tree)
```

The `flatten` method has two additional optional attributes:
- `rename_columns`: Renames the columns of the flattened dataframe to their leaf nodes (or aliases if defined in the configuration).
- `filter_null_rows`: Filters rows where all non-identifier columns have `NULL` values.
```python
df_flattened = Flatten.flatten(df, root_tree, rename_columns=True, filter_null_rows=True)
```