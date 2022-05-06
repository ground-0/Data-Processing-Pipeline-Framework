
def filter_function(row):

    if(row.sex == "M"):
        return True
    else:
        return False

def transform_function(row):

    row.state = "NA"
    print("a")
    return row

def validator_function(row):

    return False
