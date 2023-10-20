from pyspark.sql import DataFrame

def filter_data(df, column, values):
    return df.filter(df[column].isin(values))

def rename_columns(df, column_mapping):
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df
