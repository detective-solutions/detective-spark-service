# standard modules

# third party modules
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

# project related modules


class PipelineFunctions:
    def __init(self):
        pass

    @staticmethod
    def select_columns(df, conditions):
        try:
            # reduce only to columns which appear in data set
            columns = conditions["columns"]
            columns = [c for c in columns if c in df.columns]

            # in case at least one column is in list execute function
            # else return df as it is
            if len(columns) > 0:
                return df.select(columns)
            else:
                return df

        except (KeyError, TypeError):
            return df

    @staticmethod
    def row_filter(df, conditions):

        try:
            # check whether conditions can be valid
            values, columns, filters = list(), list(), list()
            for i, entry in enumerate(conditions["columns"]):
                if entry in df.columns:
                    columns += [conditions["columns"][i]]
                    filters += [conditions["filters"][i]]
                    values += [str(conditions["values"][i])]

            # shorten values
            val_count = len(values)
            col_count = len(columns)

            if val_count < col_count:
                columns = columns[:val_count]
                filters = filters[:val_count]

            elif col_count < val_count:
                columns = columns[:col_count]
                filters = filters[:col_count]

            filter_set = list(zip(columns, filters, values))

            condition = [f"{x[0]}{x[1]}{repr(x[2])}" for x in filter_set]

            return df.filter("(" + ")AND(".join(y for y in condition) + ")")

        except (KeyError, ValueError):
            return df

    # rename columns
    @staticmethod
    def rename_column(df, conditions):
        try:
            columns = [str(x) for x in conditions["columns"]]
            values = [str(x) for x in conditions["values"]]
            mapping = dict(zip(columns, values))

            for pair in mapping.items():
                df = df.withColumnRenamed(pair[0], pair[1])

            return df

        except KeyError:
            return df

    # count rows <i class="fas fa-poll"></i>
    @staticmethod
    def row_count(df, conditions, order_asc=False):

        try:
            # extract column name to count on
            column_name = conditions["columns"][0]

            # create counting
            result = (
                df.groupBy(column_name)
                .agg(f.count(column_name))
                .withColumnRenamed(f"count({column_name})", "count")
                .orderBy("count", ascending=order_asc)
            )

            return result

        except KeyError:
            return df

    # split column at a given value into multiple ones
    # <i class="fas fa-columns"></i>
    @staticmethod
    def split_column(df, conditions):
        # needed are the column to split, the value to split by
        column = conditions["columns"][0]
        filters = conditions["filters"][0]

        if isinstance(df.schema[column].dataType, StringType):

            # split column by regex (filters)
            split_col = f.split(df[column], filters)
            df = df.withColumn("split_col", split_col)
            df = df.withColumn("size", f.size("split_col"))

            # get the max the split array offers in order to
            # estimate the amount of columns needed
            max_row = df.agg({"size": "max"}).collect()[0]
            max_row = max_row["max(size)"]

            # create a column for each possible value
            for i in range(0, max_row):
                df = df.withColumn(f"{column}_{i}", split_col.getItem(i))

            # drop helper columns since they are not needed any longer
            df = df.drop(*["split_col", "size"])
            return df

        else:
            return df

    # group data and apply a basic method <i class="fas fa-calculator"></i>
    @staticmethod
    def group_data(df, conditions):
        df = df.groupBy(conditions["columns"]).agg(conditions["values"])

        # prepare resulting column names
        df.columns = df.columns.droplevel(0)
        df.reset_index(inplace=True)

        # create column names
        new_names = list()
        for key, values in conditions["values"].items():
            new_names += [f"{key}_{x}" for x in values]

        df.columns = df.columns.tolist()[: len(new_names) - 1] + new_names

        return df

    # sort data based on columns <i class="fas fa-sort-amount-down-alt"></i>
    @staticmethod
    def sort_data(df, conditions):
        sort = (
            lambda sort_type, column: f.col(column).asc()
            if sort_type == "ascending"
            else f.col(column).desc()
        )
        newDf = df.orderBy(
            *[
                sort(conditions["filters"][0], column)
                for column in conditions["columns"]
            ]
        )
        return newDf

    # drop a small chunk of columns <i class="fas fa-tint"></i>
    @staticmethod
    def drop_columns(df, conditions):
        return df.drop(*conditions["columns"])

    # fillna  <i class="fas fa-star-half-alt"></i>
    @staticmethod
    def fill_missing_values(df, conditions):

        mapping = dict(zip(conditions["columns"], conditions["values"]))
        return df.fillna(mapping)

    # drop duplicated rows - <i class="fas fa-capsules"></i>
    @staticmethod
    def drop_duplicates(df, conditions):
        if conditions["columns"][0] != "None":
            return df.dropDuplicates(conditions["columns"])
        else:
            return df.dropDuplicates()

    # drop unfilled or missing values - <i class="fas fa-eraser"></i>
    @staticmethod
    def dropna(df, conditions):
        if conditions["columns"][0] != "None":
            return df.dropna(thresh=1, subset=conditions["columns"])
        else:
            return df.dropna()

    # add new column with calculation from other columns
    # <i class="fas fa-puzzle-piece"></i>
    @staticmethod
    def calculate_column(df, conditions):
        """
        bla bla
        """
        # set prequisits with all required functions for normal operations
        truth_tests = {
            "==": eq,
            "!=": ne,
            ">": gt,
            "<": lt,
            "<=": le,
            ">=": ge,
        }
        creator_functions = {
            "add": add,
            "sub": sub,
            "div": truediv,
            "exp": pow,
            "modulo": mod,
            "mult": mul,
        }

        # extract information from conditions
        new_column = conditions["columns"][0]
        operation = conditions["filters"][0]
        column1 = conditions["values"][0]
        column2 = conditions["values"][1]

        # pre calculate all resulting values for the new column
        result = creator_functions[operation](df[column1], df[column2])

        if "limitations" in conditions:

            # if limitations is a key within conditions extract all mask values
            mask_type = conditions["limitations"]["filters"][0]
            mask_column = conditions["limitations"]["columns"][0]
            mask_value = conditions["limitations"]["values"][0]
            fallback = conditions["limitations"]["fallback"][0]

            # check if limiation fallback value is a column or a blank value
            fallback_column = (
                True if fallback in df.columns.tolist() else False
            )
            fallback = (
                float(fallback) if fallback.isnumeric() else str(fallback)
            )

            test = truth_tests[mask_type](df[mask_column], mask_value)

            if fallback_column:
                df[new_column] = np.where(test, result, df[fallback])
                return df
            else:
                df[new_column] = np.where(test, result, fallback)
                return df
        else:
            df[new_column] = creator_functions[operation](
                df[column1], df[column2]
            )
            return df

    @staticmethod
    def empty_handover(df, condition):
        return df