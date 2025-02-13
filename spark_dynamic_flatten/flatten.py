"""Flatten offers logic for flatten dataframe based on a FlattenTree"""

from typing import Optional, Tuple, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import ArrayType, StructType
from spark_dynamic_flatten.tree import FlattenTree

class Flatten:
    """
    This class provides logic for flatten a deeply nested dataframe based on configuration.

    Methods
    -------
    flatten()
        returns a flattened dataframe
    """
    # Constant Charater used for divide path. E.g field1#field12#field123
    SPLIT_CHAR = "#"

    @staticmethod
    def _select_to_rename(df:DataFrame, map_list:List[Tuple]) -> DataFrame:
        df = df.select(*[col(column).alias(alias) for column, alias in map_list])
        return df

    @staticmethod
    def _map_for_rename(root_node:FlattenTree) -> List[Tuple]:
        leafs = root_node.get_leafs()
        # Build map for renaming columns
        map_alias = []
        list_of_alias = []
        duplicates = []
        for leaf in leafs:
            path_of_leaf = leaf.get_path_to_node(split_char = Flatten.SPLIT_CHAR)
            if leaf.get_alias() is None:
                rename_to = leaf.get_name()
            else:
                rename_to = leaf.get_alias()
            map_alias.append((path_of_leaf, rename_to))

            # Check for duplicates
            if rename_to in list_of_alias:
                duplicates.append(rename_to)
            else:
                list_of_alias.append(rename_to)

        # Check if names are distinct
        assert len(duplicates) == 0, f"Column names of final DataFrame are not unique. Alias following fields: {duplicates}"
        return map_alias

    @staticmethod
    def _select_structtype(df:DataFrame, map_list:List[Tuple]) -> DataFrame:
        # Select fields of StructType. Every field needs to have a tuple with column_name of StrucType, field_name of child and alias.
        df = df.select("*", *[col(column).getItem(child).alias(alias) for column, child, alias in map_list])
        return df

    @staticmethod
    def _filter_null_rows(df:DataFrame, root_node:FlattenTree, rename_columns:bool) -> DataFrame:
        leafs = root_node.get_leafs()
        condition = None
        for leaf in leafs:
            if not leaf.get_is_identifier():
                if rename_columns:
                    column_name = leaf.get_name()
                else:
                    column_name = leaf.get_path_to_node()

                if condition is None:
                    condition = col(column_name).isNull()
                else:
                    condition = condition & col(column_name).isNull()

        if condition is not None:
            return df.filter(~condition)
        else:
            return df

    @staticmethod
    def flatten(df: DataFrame, root_node:FlattenTree, rename_columns:Optional[bool] = True, filter_null_rows:Optional[bool] = True) -> DataFrame:
        """
        Flattens the dataframe based on the configuration which has to be imported upfront as FlattenTree (see TreeManager).
        When rename_colums is False, the names of columns will be the complete path to field.
        If filter_null_rows is set to true the dataframe after flattening will only have rows where at least one non-key field is not null.


        Parameters
        ----------
        df : DataFrame
            DataFrame to be flattened
        root_node : FlattenTree
            TreeManager instance with imported flattten configuration
        rename_columns : bool, optional
            should the columns be renamed after flattening - either to the field-name or the alias
        filter_null_rows : bool, optional
            When a row only exist of null values besides the field marked as identifiers in config, the row will be filtered
            Rows where only key fields are filled should almost be irrelevant
        """
        # Make sure instance of TreeManager has FlattenTree as root node
        assert isinstance(root_node, FlattenTree), f"Root node has to be of type FlattenTree but its of type {type(root_node)}."
        # Get root node of Tree
        # Get tree layered for flatten method
        layered_tree = root_node.get_tree_layered()

        # Call flatten method
        df = Flatten._flatten(df, layered_tree)

        # After df was flattened, rename the columns to leaf-name or to alias of leaf in config
        # Prepare map for rename
        rename_map = Flatten._map_for_rename(root_node)
        # Rename
        if rename_columns:
            df = Flatten._select_to_rename(df, rename_map)

        # Filter out rows where all non-identifiers (non-keyfields) are null
        if filter_null_rows:
            df = Flatten._filter_null_rows(df, root_node, rename_columns)

        return df

    @staticmethod
    def _flatten(df: DataFrame, tree_layered:List[Optional[List[FlattenTree]]], index:int = 0) -> DataFrame:
        # get fields from dataframe
        fields = df.schema.fields

        # Divide fields on this level based on dataType
        array_fields = []
        struct_fields = []
        other_fields = []
        for field in fields:
            # Only columns of same layer like index are relevant.
            # Count how many split characters are in column name step over when column not on same level
            if field.name.count(Flatten.SPLIT_CHAR) != index:
                continue

            if isinstance(field.dataType, ArrayType):
                array_fields.append(field)
            elif isinstance(field.dataType, StructType):
                struct_fields.append(field)
            else:
                other_fields.append(field)

        # First explode all relevant arrays on this level
        for field in array_fields:
            column_name = field.name

            # If column is relevant read list of columns for actual index
            for node in tree_layered[index]:
                path_to_node = node.get_path_to_node(split_char = Flatten.SPLIT_CHAR)
                if path_to_node == column_name:
                    # When column was found, explode array
                    df = df.withColumn(column_name, explode_outer(col(column_name)))

        # Second select all relevant fields within StrucType on this level
        for field in struct_fields:
            column_name = field.name

            # If column is relevant read list of columns for actual index
            for node in tree_layered[index]:
                path_to_node = node.get_path_to_node(split_char = Flatten.SPLIT_CHAR)
                if path_to_node == column_name:
                    # Read columns of next level temporarily
                    df_upcoming_cols = df.select(f"{column_name}.*")
                    upcoming_cols = df_upcoming_cols.columns

                    # Check that every children is found on next Level and select them
                    # Build list of relevant children
                    relevant_children = [child.get_name() for child in node.get_children()]

                    # Check if every configured field exists in schema - Otherwise ERROR
                    for child in relevant_children:
                        if child not in upcoming_cols:
                            raise ValueError(f"Field {child} could not be found in data path {path_to_node}")
                    map_alias = [(column_name, child, f"{path_to_node}{Flatten.SPLIT_CHAR}{child}") for child in relevant_children]

                    df_temp1 = Flatten._select_structtype(df, map_alias)
                    df_temp2 = df_temp1.drop(col(column_name))

                    df = Flatten._flatten(df_temp2, tree_layered, index+1)

        # Third sanity check for leaf fields
        for field in other_fields:
            column_name = field.name

            # If column is relevant read list of columns for actual index
            for node in tree_layered[index]:
                path_to_node = node.get_path_to_node(split_char = Flatten.SPLIT_CHAR)
                if path_to_node == column_name:

                    # We are at the end of path - make sanity checks
                    assert len(node.get_children()) == 0, f"There is no more layer to flatten. Check config for path {node.get_path_to_node(Flatten.SPLIT_CHAR)}"
        # When finished for this layer, return
        return df
