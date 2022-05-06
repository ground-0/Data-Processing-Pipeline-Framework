def transform_function(row):

    if(row.sex == "M"):
        gender = "male"
    else:
        gender = "female"

    row.sex = gender

    return row