from typing import TypeVar
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, DoubleType, LongType, ShortType, ByteType, DateType, TimestampType, DecimalType, BinaryType, NullType, DataType, MapType

BASIC_SPARK_TYPES = TypeVar(StringType,  # pylint: disable=C0103
                            IntegerType,
                            FloatType,
                            BooleanType,
                            DoubleType,
                            LongType,
                            ShortType,
                            ByteType,
                            DateType,
                            TimestampType,
                            DecimalType,
                            BinaryType,
                            NullType,
                            DataType
                            )

def get_pyspark_sql_type(typename: str) -> DataType:
    """
    Maps the python type to spark SQL data type

    Parameters
    ----------
    typename : str
        python name of the data type

    Returns
    ----------
    DataType
        Corresponding spark SQL data type to the 
    """
    type_mapping = {
        "string": StringType,
        "integer": IntegerType,
        "float": FloatType,
        "boolean": BooleanType,
        "double": DoubleType,
        "long": LongType,
        "short": ShortType,
        "byte": ByteType,
        "date": DateType,
        "timestamp": TimestampType,
        "decimal": DecimalType,
        "binary": BinaryType,
        "null": NullType,
        "map": MapType,
    }
    if typename in type_mapping:
        return type_mapping[typename]()
    else:
        raise ValueError(f"Unsupported type name: {typename}")