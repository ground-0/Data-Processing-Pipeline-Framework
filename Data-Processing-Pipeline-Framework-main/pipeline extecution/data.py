from abc import abstractmethod
import pandas as pd
from sqlalchemy import create_engine
from typing import Dict, List, Tuple
from pandas.io import sql
import mysql.connector as mysql


class DataClass:

  type = "BaseClass"

  def __init__(self, name):
    self.name = name

  def fetch_name(self) -> str:
    return self.name
  
  def get_type(self) -> str:
    return self.type

  @abstractmethod
  def fetch_data(self):
    pass


class DataTable(DataClass):

  type = "DataTable"

  def __init__(self, name, df: pd.DataFrame):
    self.name = name
    engine = create_engine('sqlite:///data.db', echo=False)
    df.to_sql(name="DPPF" + self.name, con=engine, if_exists="replace", index=False)
    engine.dispose()

  def fetch_data(self) -> pd.DataFrame:
    engine = create_engine('sqlite:///data.db', echo=False)
    df = pd.read_sql_table("DPPF" + self.name, con=engine)
    engine.dispose()
    return df

  def clean_up(self):
    engine = create_engine('sqlite:///data.db', echo=False)
    table_name = "DPPF"+self.name
    sql.execute('DROP TABLE IF EXISTS %s'%table_name, engine)
    sql.execute('VACUUM', engine)
    engine.dispose()


class DataVariable(DataClass):

  type = "DataVariable"

  def __init__(self, name, value: float):
    self.name = name
    self.value = value

  def fetch_data(self) -> float:
    return self.value


class DataDict:
  
  def __init__(self, input_dt: List[DataTable]):
    self.data_dict:Dict[str, DataTable] = {}
    for dt in input_dt:
      self.data_dict[dt.fetch_name()] = dt

  def edit_dt(self, dt: DataClass) -> bool:
    self.data_dict[dt.fetch_name()] = dt
    return True

  def fetch_dt(self, name: str) -> DataClass:
    return self.data_dict[name]

  def clean_up(self):
    for dt in self.data_dict.values():
      if dt.get_type() == "DataTable":
        dt.clean_up()


class DataReader:

  @staticmethod
  def generate_csv_data_dict(file_path: str, table_name: str):
    df = pd.read_csv(file_path)
    dt = DataTable(table_name, df)
    return DataDict([dt])

  @staticmethod
  def generate_excel_data_dict(file_path: str):
    df_dict = pd.read_excel(file_path, sheet_name=None)
    dt_list = []
    for sheet in df_dict:
      dt_list.append(DataTable(sheet, df_dict[sheet]))
    return DataDict(dt_list)

  @staticmethod
  def generate_sql_data_dict(host: str, db_name: str, username: str, password: str, table_name_list: List[str]):
    db_connection = mysql.connect(host=host, database=db_name, user=username, password=password)
    dt_list = []
    for table_name in table_name_list:
      df = pd.read_sql('SELECT * FROM ' + table_name, con=db_connection)
      dt_list.append(DataTable(table_name, df))
    return DataDict(dt_list)

  @staticmethod
  def generate_data_dict(data_sources: List[Tuple[str, Dict]]):
    dt_list = []
    for source in data_sources:
      if source[0] == "csv":
        df = pd.read_csv(source[1]["file_path"])
        dt = DataTable(source[1]["table_name"], df)
        dt_list.append(dt)
      elif source[0] == "excel":
        df_dict = pd.read_excel(source[1]["file_path"], sheet_name=None)
        for sheet in df_dict:
          dt_list.append(DataTable(sheet, df_dict[sheet]))
      elif source[0] == "sql":
        db_connection = mysql.connect(host=source[1]["host"], database=source[1]["db_name"], user=source[1]["username"], password=source[1]["password"])
        for table_name in source[1]["table_name_list"]:
          df = pd.read_sql('SELECT * FROM ' + table_name, con=db_connection)
          dt_list.append(DataTable(table_name, df))
    return DataDict(dt_list)