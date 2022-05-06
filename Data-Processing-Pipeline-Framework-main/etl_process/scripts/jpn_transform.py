

def transform_function(row):
  if "Ord_id" in row:
    row["Ord_id"] = "JPN_" + str(row["Ord_id"])
  if "Cust_id" in row:
    row["Cust_id"] = "JPN_" + str(row["Cust_id"])

  return row