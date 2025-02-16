from spark_dynamic_flatten import TreeManager
# from spark_dynamic_flatten.flatten import Flatten

# Create trees based on schema json
tree_schema1 = TreeManager.from_schema_json_file("examples/formula1_schema.json")
tree_schema2 = TreeManager.from_schema_json_file("examples/formula1_schema.json")

# Check if trees are qual
equal, differences = tree_schema1.equals(tree_schema2)

if equal is False:
    print(differences)
else:
    print("Trees are equal")

# Generate flatten config from schema1
flatten_config = tree_schema1.generate_fully_flattened_paths()
tree_flatten = TreeManager.from_flatten_dict(flatten_config)
# Print tree
tree_flatten.print_tree()

# When spark cluster is available and dataframe with formula1 data is loaded
# The dataframe could be flattened like this:
# df_flattened = Flatten.flatten(dataframe, tree_flatten)
