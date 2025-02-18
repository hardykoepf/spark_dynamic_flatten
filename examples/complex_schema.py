from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType, FloatType, BooleanType
)
from spark_dynamic_flatten import TreeManager

# Define the schema
schema = StructType([
    StructField("level1", StructType([
        StructField("level2", StructType([
            StructField("level3", StructType([
                StructField("name", StringType(), True),
                StructField("attributes", MapType(StringType(), IntegerType()), True),
                StructField("values", ArrayType(StringType()), True),
                StructField("level4", StructType([
                    StructField("level5", StructType([
                        StructField("level6", StructType([
                            StructField("id", IntegerType(), True),
                            StructField("description", StringType(), True),
                            StructField("metrics", MapType(StringType(), FloatType()), True),
                            StructField("flags", ArrayType(BooleanType()), True)
                        ]), True)
                    ]), True)
                ]), True),
                StructField("additional_info", ArrayType(StructType([
                    StructField("info_id", IntegerType(), True),
                    StructField("info_description", StringType(), True)
                ])), True)
            ]), True)
        ]), True)
    ]), True)
])

tree = TreeManager.from_struct_type(schema)
tree.print_tree()
print(tree.generate_fully_flattened_struct())