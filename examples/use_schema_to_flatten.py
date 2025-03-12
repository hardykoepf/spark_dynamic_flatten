from spark_dynamic_flatten import TreeManager
# from spark_dynamic_flatten.flatten import Flatten

# Create trees based on schema json
tree_schema1 = TreeManager.from_schema_json_file("examples/formula1_schema.json")

# When you want to use the schema, you can create a json to be used for flatten the struct in complete
# This will generate the config json for flattening which you can store as file and modify to your needs.
flatten_config = tree_schema1.generate_fully_flattened_struct()

# This generated config you can use for creating a FlattenTree which is used to flatten
# In real world, this library is not intended to full flatten a Dataframe (but of course you can)
# It's more that you selectively/partly flatten data so that not all has to be flattened (performance)
tree_flatten = TreeManager.from_flatten_dict(flatten_config)
# Print tree
tree_flatten.print()

tree = tree_flatten.to_tree()
tree.print()

# Example of flattening a dataframe (uncomment when spark cluster is available)
# df_flattened = Flatten.flatten(dataframe, tree_flatten)
