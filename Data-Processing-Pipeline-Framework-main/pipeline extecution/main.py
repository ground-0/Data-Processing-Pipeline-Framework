import sys
import pipeline_structure as ps
import pandas as pd
import Specification as spec
import mysql.connector as mysql
from sqlalchemy import create_engine
from pandas.io import sql

def DPPF_run(specification_file):
  print(pd.__version__)
  specification = spec.Specification(specification_file)
  input_dict = specification.getInputDict()
  p_format = specification.getPipelineFormat()
  pipeline1 = ps.Pipeline(input_dict, p_format)
  output = pipeline1.execute()
  outputList = specification.getOutputDict()

  for o in outputList:

    if(o[0] == 'sql'):
      # sql output code
      # db_connection = mysql.connect(host=o[1]["host"], database=o[1]["db_name"], user=o[1]["username"], password=o[1]["password"])
      url = "mysql://" + o[1]["username"] + ":" + o[1]["password"] + "@" + o[1]["host"] + "/" + o[1]["db_name"] 
      engine = create_engine(url)
      db_connection = engine.connect()
      for table_name in o[1]["tables"]:
        df = output.fetch_dt(table_name).fetch_data()
        df.to_sql( con=db_connection, name=table_name, if_exists='replace')
        
        
    elif (o[0] == 'excel'):
      df_list = []
      for table_name in o[1]["tables"]:
        df = output.fetch_dt(table_name).fetch_data()
        df_list.append(df)
      writer = pd.ExcelWriter(o[1]["file_path"], engine='xlsxwriter')   
      for dataframe, sheet in zip(df_list, o[1]["tables"]):
        dataframe.to_excel(writer, sheet_name=sheet, startrow=0 , startcol=0)   
      writer.save()      
    elif (o[0] == 'csv'):
      df = output.fetch_dt(o[1]["table_name"]).fetch_data()
      df.to_csv(o[1]["file_path"])


if __name__ == "__main__":
  # Erases the data.db file
  open('data.db', 'w').close()
  
  DPPF_run(sys.argv[1])