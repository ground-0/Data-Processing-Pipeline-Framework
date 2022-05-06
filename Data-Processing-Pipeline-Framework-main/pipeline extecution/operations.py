import importlib
from abc import abstractmethod
from datetime import datetime
import data
import pandas as pd
import re
from sqlite3 import connect
import os, sys


def schema_params(required_params: dict, operation_params: dict) -> bool:
  '''
  required_params = {params : type}
  operation_params = {params : value}
  '''
  if len(required_params) != len(operation_params):
    return False
  for k, v in operation_params.items():
    if required_params.get(k) == None:
      return False
    elif type(v) != required_params[k]:
      return False
  return True


class BaseOperation:


  @staticmethod
  @abstractmethod
  def validate_params(operation_params: dict) -> bool:
    pass

  @staticmethod
  def get_type() -> str:
    return "BaseOperation"

  @classmethod
  def get_name(cls) -> str:
    return cls.__name__


class SimpleOperation(BaseOperation):

  @staticmethod
  @abstractmethod
  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str) -> data.DataTable:
    pass

  @staticmethod
  def get_type() -> str:
    return "SimpleOperation"


class ComplexOperation(BaseOperation):

  @staticmethod
  @abstractmethod
  def run(operation_params: dict, inp_dd: data.DataDict, output_name: str) -> data.DataTable:
    pass

  @staticmethod
  def get_type() -> str:
    return "ComplexOperation"


class AggregateOperation(BaseOperation):

  @staticmethod
  @abstractmethod
  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str) -> data.DataVariable:
    pass

  @staticmethod
  def get_type() -> str:
    return "AggregateOperation"

class SimpleValidation(BaseOperation):

  @staticmethod
  @abstractmethod
  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None) -> bool:
    pass

  @staticmethod
  def get_type() -> str:
    return "SimpleValidation"

class StdAggregateOperation(AggregateOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()
    if df[operation_params["column_name"]].dtype == 'int64' or df[operation_params["column_name"]].dtype == 'float64':
      return data.DataVariable(output_name, df[operation_params["column_name"]].std())
    else:
      raise Exception("The datatype of the column should by either int or float")

class MedianAggregateOperation(AggregateOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    if df[operation_params["column_name"]].dtype == 'int64' or df[operation_params["column_name"]].dtype == 'float64':
      return data.DataVariable(output_name, df[operation_params["column_name"]].median())
    else:
      raise Exception("The datatype of the column should by either int or float")

class ModeAggregateOperation(AggregateOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    if df[operation_params["column_name"]].dtype == 'int64' or df[operation_params["column_name"]].dtype == 'float64':
      return data.DataVariable(output_name, df[operation_params["column_name"]].mode())
    else:
      raise Exception("The datatype of the column should by either int or float")

class SumAggregateOperation(AggregateOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    if df[operation_params["column_name"]].dtype == 'int64' or df[operation_params["column_name"]].dtype == 'float64':
      return data.DataVariable(output_name, df[operation_params["column_name"]].sum())
    else:
      raise Exception("The datatype of the column should by either int or float")

class MinAggregateOperation(AggregateOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    if df[operation_params["column_name"]].dtype == 'int64' or df[operation_params["column_name"]].dtype == 'float64':
      return data.DataVariable(output_name, df[operation_params["column_name"]].min())
    else:
      raise Exception("The datatype of the column should by either int or float")

class MaxAggregateOperation(AggregateOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    if df[operation_params["column_name"]].dtype == 'int64' or df[operation_params["column_name"]].dtype == 'float64':
      return data.DataVariable(output_name, df[operation_params["column_name"]].max())
    else:
      raise Exception("The datatype of the column should by either int or float")

class CountAggregateOperation(AggregateOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()
    if df[operation_params["column_name"]].dtype == 'object':
      dt = data.DataTable(output_name, df.groupby(operation_params['column_name']).size().reset_index(name='counts'))
      return dt
    else:
      raise Exception("The datatype of the column should by either int or float")


class IntegerValidation(SimpleValidation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str, "comparator": str, "value": int}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    return df[operation_params["column_name"]].apply(lambda x: eval(str(x)+operation_params["comparator"]+str(operation_params["value"]))).all()

class StringLenValidation(SimpleValidation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str, "comparator": str, "value": int}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    return df[operation_params["column_name"]].apply(lambda x: eval(str(len(str(x)))+operation_params["comparator"]+str(operation_params["value"]))).all()

class CustomValidation(SimpleValidation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"python_souce": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      return Exception("python source missing")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):

    df = inp_dt.fetch_data()
    sys.path.append(os.path.join(os.path.dirname(sys.path[0]), operation_params["path"]))
    validator_function = __import__(operation_params["python_source"]).validator_function

    # col = operation_params['column_name']
    # df[col] = df[col].map(transform_function)

    return df.apply(validator_function, axis=1).all()  


class RegExValidation(SimpleValidation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str, "regex": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    return df[operation_params["column_name"]].apply(lambda x: bool(re.match(operation_params["regex"], x))).all()

class LowerCaseValidation(SimpleValidation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    return df[operation_params["column_name"]].apply(lambda x: x.islower()).all()

class UpperCaseValidation(SimpleValidation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    return df[operation_params["column_name"]].apply(lambda x: x.isupper()).all()

class UniqueColumnValidation(SimpleValidation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    return df[operation_params["column_name"]].is_unique

class DateValidation(SimpleValidation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str, "format": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str=None):
    df = inp_dt.fetch_data()
    return df[operation_params["column_name"]].apply(lambda x: bool(datetime.strptime(x, operation_params['format']))).all()
    

class RenameColumnOperation(SimpleOperation):
  
  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "new_column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()
    df[operation_params["new_column_name"]] = df[operation_params["column_name"]]
    df.pop(operation_params["column_name"])
    dt = data.DataTable(output_name, df)

    return dt

class DeleteColumnOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()
    df.pop(operation_params["column_name"])
    dt = data.DataTable(output_name, df)
    return dt

class InsertColumnOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    optional_param = "default_value"
    required_param = "column_name"

    if required_param in operation_params.keys() and optional_param in operation_params.keys():
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()
    df[operation_params["column_name"]] = operation_params["default_value"]
    dt = data.DataTable(output_name, df)
    return dt

class LowerCaseOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x.lower())
    dt = data.DataTable(output_name, df)

    return dt
  
class UpperCaseOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x.upper())

    dt = data.DataTable(output_name, df)
    return dt

class AddValueOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "value": int}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x + operation_params["value"])

    dt = data.DataTable(output_name, df)
    return dt


class AddVariableOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"table_name": str, "column_name": str, "variable": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dd: data.DataDict, output_name: str):
    df = inp_dd.fetch_dt(operation_params["table_name"]).fetch_data()

    value = inp_dd.fetch_dt(operation_params["variable"]).fetch_data()
    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x + value)

    dt = data.DataTable(output_name, df)
    return dt


class SubVariableOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"table_name": str, "column_name": str, "variable": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dd: data.DataDict, output_name: str):
    df = inp_dd.fetch_dt(operation_params["table_name"]).fetch_data()

    value = inp_dd.fetch_dt(operation_params["variable"]).fetch_data()
    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x - value)

    dt = data.DataTable(output_name, df)
    return dt


class MultiplyValueOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "value": int, "precision": int}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: round(x * operation_params["value"], operation_params["precision"]))

    dt = data.DataTable(output_name, df)
    return dt

class MultiplyVariableOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"table_name": str, "column_name": str, "variable": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dd: data.DataDict, output_name: str):
    df = inp_dd.fetch_dt(operation_params["table_name"]).fetch_data()

    value = inp_dd.fetch_dt(operation_params["variable"]).fetch_data()
    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x*value)

    dt = data.DataTable(output_name, df)
    return dt

class DivideValueOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "value": int, "precision": int}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: round(x / operation_params["value"], operation_params["precision"]))

    dt = data.DataTable(output_name, df)
    return dt

class AddColumnsOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    if operation_params.get("inplace") != None:
      if operation_params["inplace"] == False:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "output_column":str}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
      else:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
    else:
      raise Exception("Either parameter column_name is not present or given more parameters")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    if(operation_params.get("inplace") == True):
      df[operation_params["column_name1"]] = df[operation_params["column_name1"]] + df[operation_params["column_name2"]]
    else:
      df[operation_params["output_column"]] = df[operation_params["column_name1"]] + df[operation_params["column_name2"]]

    dt = data.DataTable(output_name, df)
    return dt

class MultiplyColumnsOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    if operation_params.get("inplace") != None:
      if operation_params["inplace"] == False:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "output_column":str, "precision": int}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
      else:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "precision": int}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
    else:
      raise Exception("Either parameter column_name is not present or given more parameters")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    if(operation_params.get("inplace") == True):
      df[operation_params["column_name1"]] = df[operation_params["column_name1"]] * df[operation_params["column_name2"]]
      df[operation_params["column_name1"]] = df[operation_params["column_name1"]].round(operation_params["precision"])
    else:
      df[operation_params["output_column"]] = df[operation_params["column_name1"]] * df[operation_params["column_name2"]]
      df[operation_params["column_name1"]] = df[operation_params["column_name1"]].round(operation_params["precision"])

    dt = data.DataTable(output_name, df)
    return dt

class DivideColumnsOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    if operation_params.get("inplace") != None:
      if operation_params["inplace"] == False:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "output_column":str, "precision": int}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
      else:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "precision": int}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
    else:
      raise Exception("Either parameter column_name is not present or given more parameters")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    if(operation_params.get("inplace") == True):
      df[operation_params["column_name1"]] = df[operation_params["column_name1"]] / df[operation_params["column_name2"]]
      df[operation_params["column_name1"]] = df[operation_params["column_name1"]].round(operation_params["precision"])
    else:
      df[operation_params["output_column"]] = df[operation_params["column_name1"]] / df[operation_params["column_name2"]]
      df[operation_params["column_name1"]] = df[operation_params["column_name1"]].round(operation_params["precision"])

    dt = data.DataTable(output_name, df)
    return dt

class LengthOfTextOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "output_column": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    if(df[operation_params["column_name"]].dtype == 'object'):
      df[operation_params["output_column"]] = df[operation_params["column_name"]].apply(lambda x: len(x))
    else:
      raise Exception("Cannot apply length operation on non-string values")

    dt = data.DataTable(output_name, df)
    return dt

class DateDiffOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name1": str, "column_name2": str, "output_column": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either parameter column_name is not present or given more parameters")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    df[operation_params["output_column"]] = df[operation_params["column_name2"]].sub(df[operation_params["column_name1"]], axis=0)
    df[operation_params["output_column"]] = df[operation_params["output_column"]].apply(lambda x: int(x.total_seconds()))

    dt = data.DataTable(output_name, df)
    return dt

class AddDateOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "date": datetime}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x - datetime.fromtimestamp(0) + operation_params["date"])

    dt = data.DataTable(output_name, df)
    return dt

class SplitColumnOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "split_by": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    st = df[operation_params["column_name"]].iloc[0]
    st_list = st.split(operation_params["split_by"])
    st_len = len(st_list)

    list = [operation_params["column_name"]+str(i+1) for i in range(st_len)]

    df[list] = df[operation_params["column_name"]].apply(lambda x: pd.Series(str(x).split(operation_params["split_by"])))

    dt = data.DataTable(output_name, df)
    return dt


class JoinTableOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:
    
    required_params = {"table1":str, "table2":str, "left_on":str, "right_on":str, "type":str}
    if schema_params(required_params, operation_params):
      return True
    return Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dd: data.DataDict, output_name: str):
    df1 = inp_dd.fetch_dt(operation_params["table1"]).fetch_data()
    df2 = inp_dd.fetch_dt(operation_params["table2"]).fetch_data()
    dt = data.DataTable(
      output_name,
      df1.merge(
        df2, left_on=operation_params["left_on"], right_on=operation_params["right_on"], how=operation_params["type"]))
    return dt

class CustomSqlOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"output_table":str, "sql_file":str}
    if schema_params(required_params, operation_params):
      return True
    return Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dd: data.DataDict, output_name: str):

    conn = connect(':memory:')

    for data_name in inp_dd.data_dict:
      df = inp_dd.fetch_dt(data_name).fetch_data()
      df.to_sql(data_name, conn)

    cursor = conn.cursor()

    for line in open(operation_params['sql_file']):
      cursor.execute(line)

    df = pd.read_sql(f"select * from {operation_params['output_table']}", conn)

    dt = data.DataTable(output_name, df)

    conn.close()

    return dt

class IntFilterOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:
    optional_params = ["less_than_equal_to", "greater_than_equal_to", "less_than", "greater_than", "equal_to"]
    required_params = ["column_name"]
    if operation_params["column_name"] != None:
      has_optional_param = False
      for optional_param in optional_params:
        if operation_params.get(optional_param) != None: 
          if type(operation_params[optional_param]) in [int, float]:
            has_optional_param = True
      if has_optional_param:
        return True
    return Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    if df[operation_params["column_name"]].dtype == 'int64' or df[operation_params["column_name"]].dtype == 'float64':
      if operation_params.get('less_than_equal_to') != None:
        df = df[df[operation_params['column_name']] <= operation_params['less_than_equal_to']]

      if operation_params.get('greater_than_equal_to') != None:
        df = df[df[operation_params['column_name']] >= operation_params['greater_than_equal_to']]

      if operation_params.get('less_than') != None:
        df = df[df[operation_params['column_name']] < operation_params['less_than']]

      if operation_params.get('greater_than') != None:
        df = df[df[operation_params['column_name']] > operation_params['greater_than']]

      if operation_params.get('equal_to') != None:
        df = df[df[operation_params['column_name']] == operation_params['equal_to']]

    else:
      raise Exception("Either column type should be integer or a decimal")
    
    dt = data.DataTable(output_name, df)
    return dt
    
class CustomFilterOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    if (operation_params["python_source"] != None):
      return True
    else:
      return Exception("Python source missing")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):

    df = inp_dt.fetch_data()
    sys.path.append(os.path.join(os.path.dirname(sys.path[0]), operation_params["path"]))
    filter_function = __import__(operation_params["python_source"]).filter_function

    df = df [ df.apply(filter_function, axis=1)]

    dt = data.DataTable(output_name,df)
    return dt

class CustomTransformOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    if (operation_params["python_source"] != None):
      return True
    else:
      return Exception("Either one of the parameters are missing")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):

    df = inp_dt.fetch_data()

    sys.path.append(os.path.join(os.path.dirname(sys.path[0]), operation_params["path"]))
    transform_function = importlib.import_module(operation_params["python_source"]).transform_function

    # col = operation_params['column_name']
    # df[col] = df[col].map(transform_function)

    df = df.apply(transform_function, axis=1)

    dt = data.DataTable(output_name,df)
    return dt


class MeanAggregationOperation(AggregateOperation):

  def validate_params(operation_params: dict) -> bool:
    
    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str) -> data.DataVariable:

    df = inp_dt.fetch_data()

    value = sum(df[operation_params["column_name"]])/len(df[operation_params["column_name"]])

    dv = data.DataVariable(output_name, value)
    return dv

class DeleteDuplicatesOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:
      return True

  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()

    df = df.drop_duplicates(keep='first',inplace=False)
    dt = data.DataTable(output_name, df)
    return dt

class RegExFilter(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str, "regex": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable, output_name: str):
    df = inp_dt.fetch_data()
    df = df[df[operation_params["column_name"]].str.match(operation_params["regex"])]
    dt = data.DataTable(output_name, df)
    return dt

class AppendTablesOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"input_tables":list}
    if schema_params(required_params, operation_params):
      return True
    return Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dd: data.DataDict, output_name: str):

    df = inp_dd.fetch_dt(operation_params["input_tables"][0]).fetch_data()

    for i in range(1, len(operation_params["input_tables"])):
      df2 = inp_dd.fetch_dt(operation_params["input_tables"][i]).fetch_data()
      df = df.append(df2, ignore_index = True)

    dt = data.DataTable(output_name, df)
    return dt