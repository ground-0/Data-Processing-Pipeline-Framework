

def transform_function(row):
  if "Ord_id" in row:
    row["Ord_id"] = "USA_" + str(row["Ord_id"])
  if "Cust_id" in row:
    row["Cust_id"] = "USA_" + str(row["Cust_id"])

  return row