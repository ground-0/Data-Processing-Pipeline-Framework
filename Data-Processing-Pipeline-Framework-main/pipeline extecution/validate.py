import pandas as pd

def filter_function(row):

    if(row.sex == "M"):
        return True
    else:
        return False

def transform_function(row):

    row.salary += 1

    return row

def validator_function(row):

    row["School completion date"] = pd.to_datetime(row["School completion date"], format='%d/%m/%Y')

    if row["School completion date"] < "31/05/2022":
        return True
    return False