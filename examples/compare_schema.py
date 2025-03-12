from spark_dynamic_flatten import TreeManager
# from spark_dynamic_flatten.flatten import Flatten

# Create trees based on schema json
tree_schema1 = TreeManager.from_schema_json_file("examples/formula1_schema.json")
tree_schema2 = TreeManager.from_schema_json_file("examples/formula1_schema.json")

# To print the tree in console:
tree_schema1.print()

# Check if trees are qual
if not tree_schema1.equals(tree_schema2):
    tree_difference = tree_schema1.subtract(tree_schema2)
    tree_difference.print()
    # You can also create a list of differences:
    print(tree_difference.get_tree_as_list())
else:
    print("Trees are equal")
