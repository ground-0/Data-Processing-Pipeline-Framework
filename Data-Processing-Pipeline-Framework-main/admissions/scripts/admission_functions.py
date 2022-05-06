import datetime

def transform_function(row):
  row["date of application"] = datetime.datetime.strptime(row["date of application"], "%m/%d/%Y").strftime("%d/%m/%Y")
  row["School completion date"] = datetime.datetime.strptime(row["School completion date"], "%m/%d/%Y").strftime("%d/%m/%Y")
  row["Gender"] = "M" if row["Gender"] == "Male" else "F"
  num_list = row["ContactNo"].split("-")
  row["ContactNo"] = num_list[0] + "-" + num_list[1] + num_list[2] + num_list[3]

  return row

def filter_function(row):
  return (len(row["name"].split(" ")) == 2) and (len(row["Father name"].split(" ")) == 2) and (len(row["Mother name"].split(" ")) == 2)


def validator_function(row):
  date = row["School completion date"].split("/")
  d1 = datetime.datetime(int(date[2]), int(date[1]), int(date[0]))
  d2 = datetime.datetime(2022, 5, 31)
  if d1 < d2:
      return True
  return False