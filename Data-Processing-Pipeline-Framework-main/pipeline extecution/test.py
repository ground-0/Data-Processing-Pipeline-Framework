from datetime import datetime
import data
import pandas as pd
import pipeline_structure as ps


def test1(spec_pth="",f_pth=""):
  df1 = pd.DataFrame({
    "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardan"],
    "value1": [10, 20, 30, 40],
    "key1": [1, 2, 3, 4],
  })
  df2 = pd.DataFrame({
    "name2": ["Rohit", "Tarun", "Jishnu", "Harshvardan"],
    "value2": [10, 20, 30, 40],
    "key2": [1, 2, 3, 4],
  })


  input_table1 = data.DataTable("Data1", df1)
  input_table2 = data.DataTable("Data2", df2)
  input_data_dict = data.DataDict([input_table1, input_table2])

  p_format = ps.PipelineFormat("SimplePipeline")
  s1_format = ps.StageFormat("Stage1")
  s2_format = ps.StageFormat("Stage2")

  t_format1 = ps.TaskFormat(["Data1"], "LowerCase", {"column_name": "name1"}, "Data3")
  t_format2 = ps.TaskFormat(["Data3"], "AddValue", {"column_name": "value1", "value": 10}, "Data4")
  t_format3 = ps.TaskFormat(["Data2"], "UpperCase", {"column_name": "name2"}, "Data5")
  t_format4 = ps.TaskFormat(["Data5"], "AddValue", {"column_name": "value2", "value": -10}, "Data6")
  t_format5 = ps.TaskFormat(["Data4"], "MeanAggregation", {"column_name": "value1"}, "Data7")
  t_format6 = ps.TaskFormat(["Data4"], "AddVariable", {"table_name": "Data4", "column_name": "value1", "variable": "Data7"}, "Data8")
  t_format7 = ps.TaskFormat(["Data8", "Data6"], "JoinTableOperation", {"table1": "Data8", "table2": "Data6", "left_on": "key1", "right_on": "key2", "type": "inner"}, "Data9")


  s1_format.add_task_format(t_format1)
  s1_format.add_task_format(t_format2)
  s1_format.add_task_format(t_format3)
  s1_format.add_task_format(t_format4)
  s2_format.add_task_format(t_format5)
  s2_format.add_task_format(t_format6)
  s2_format.add_task_format(t_format7)

  p_format.add_stage_format(s1_format)
  p_format.add_stage_format(s2_format)

  pipeline1 = ps.Pipeline(input_data_dict, p_format)
  output = pipeline1.execute()
  print(output.fetch_dt("Data9").fetch_data())

def test2(spec_pth="",f_pth=""):
  input_data_dict = data.DataReader.generate_csv_data_dict("./sample_data/random_data.csv", "Data1")
  p_format = ps.PipelineFormat("SimplePipeline")
  s_format = ps.StageFormat("Stage1")

  
  t_format1 = ps.TaskFormat(["Data1"], "LowerCase", {"column_name": "firstname"}, "Data2")
  t_format2 = ps.TaskFormat(["Data2"], "UpperCase", {"column_name": "lastname"}, "Data3")
  t_format3 = ps.TaskFormat(["Data3"], "AddValue", {"column_name": "salary", "value": 100}, "Data4")

  s_format.add_task_format(t_format1)
  s_format.add_task_format(t_format2)
  s_format.add_task_format(t_format3)

  p_format.add_stage_format(s_format)
  pipeline1 = ps.Pipeline(input_data_dict, p_format)
  output = pipeline1.execute()
  print(output.fetch_dt("Data4").fetch_data())

def test3(spec_pth="",f_pth=""):
  input_data_dict = data.DataReader.generate_excel_data_dict("./sample_data/market_star_schema.xlsx")
  p_format = ps.PipelineFormat("SimplePipeline")
  s_format = ps.StageFormat("Stage1")

  
  t_format1 = ps.TaskFormat(["market_fact_table"], "AddValue", {"column_name": "Discount", "value": 100}, "Data1")

  s_format.add_task_format(t_format1)

  p_format.add_stage_format(s_format)
  pipeline1 = ps.Pipeline(input_data_dict, p_format)
  output = pipeline1.execute()
  print(output.fetch_dt("Data1").fetch_data())

def test4(spec_pth="./pipeline\ execution/pipeline1.xml",f_pth=""):
  database="companydb"
  username="root"
  password="only4dbms"
  input_data_dict = data.DataReader.generate_sql_data_dict("localhost", database, username, password, ["department", "employee"])
  p_format = ps.PipelineFormat("SimplePipeline")
  s_format = ps.StageFormat("Stage1")

  t_format = ps.TaskFormat(["Data1"], "CustomTransform", {"python_source": "filter", "path":"admissions/scripts"}, "Data1")
  t_format1 = ps.TaskFormat(["employee"], "CustomTransform", {"python_source": "filter", "path":"admissions/scripts"}, "Data1")

  s_format.add_task_format(t_format1)
  s_format.add_task_format(t_format)


  p_format.add_stage_format(s_format)

  # specification = spec.Specification(spec_pth)
  # e = specification.getInputDict()
  # p_format = specification.getPipelineFormat()
  pipeline1 = ps.Pipeline(input_data_dict, p_format)
  output = pipeline1.execute()
  print(output.fetch_dt("Data1").fetch_data())


def test5(spec_pth="",f_pth=""):
  df1 = pd.DataFrame({
    "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardan"],
    "value1": [10, 20, 30, 40],
    "key1": [1, 2, 3, 4],
  })

  input_table1 = data.DataTable("Data1", df1)
  input_data_dict = data.DataDict([input_table1])

  p_format = ps.PipelineFormat("SimplePipeline")
  s1_format = ps.StageFormat("Stage1")

  t_format1 = ps.TaskFormat(["Data1"], "IntegerValidation", {"column_name": "value1", "comparator": ">", "value": 10})
  t_format2 = ps.TaskFormat(["Data1"], "LowerCase", {"column_name": "name1"}, "Data2")


  s1_format.add_task_format(t_format1)
  s1_format.add_task_format(t_format2)

  p_format.add_stage_format(s1_format)

  pipeline1 = ps.Pipeline(input_data_dict, p_format)
  pipeline1.execute()


def test6(spec_pth="",f_pth=""):
  df1 = pd.DataFrame({
    "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardan"],
    "value1": [10, 20, 30, 40],
    "key1": [1, 2, 3, 4],
  })

  def pipeline_success_callback(pipeline):
    print("custom_pipeline_success_callback")
    pipeline.clean_up()


  input_table1 = data.DataTable("Data1", df1)
  input_data_dict = data.DataDict([input_table1])

  p_format = ps.PipelineFormat("SimplePipeline")
  p_format.set_success_callback(pipeline_success_callback)
  
  s1_format = ps.StageFormat("Stage1")

  t_format1 = ps.TaskFormat(["Data1"], "AddValue", {"column_name": "value1", "value": 10}, "Data2")
  t_format2 = ps.TaskFormat(["Data2"], "LowerCase", {"column_name": "name1"}, "Data3")

  s1_format.add_task_format(t_format1)
  s1_format.add_task_format(t_format2)

  p_format.add_stage_format(s1_format)

  pipeline1 = ps.Pipeline(input_data_dict, p_format)
  pipeline1.execute()

def test_multi_source():
  data_source = [
    ("csv", {"file_path": "./sample_data/random_data.csv", "table_name": "Data1"}),
    ("excel", {"file_path": "./sample_data/market_star_schema.xlsx"}),
  ]
  input_data_dict = data.DataReader.generate_data_dict(data_source)

  p_format = ps.PipelineFormat("SimplePipeline")
  s1_format = ps.StageFormat("Stage1")
  t_format1 = ps.TaskFormat(["Data1"], "AddValue", {"column_name": "salary", "value": 10}, "Data2")
  t_format2 = ps.TaskFormat(["market_fact_table"], "AddValue", {"column_name": "Discount", "value": 100}, "Data3")
  s1_format.add_task_format(t_format1)
  s1_format.add_task_format(t_format2)

  p_format.add_stage_format(s1_format)

  pipeline1 = ps.Pipeline(input_data_dict, p_format)
  output = pipeline1.execute()
  print(output.fetch_dt("Data2").fetch_data())
  print(output.fetch_dt("Data3").fetch_data())


class LowerCase_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "LowerCase", {"column_name": "name1"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit", "tarun", "jishnu", "harshvardhan"],
            "value1": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class UpperCase_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "UpperCase", {"column_name": "name1"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["ROHIT", "TARUN", "JISHNU", "HARSHVARDHAN"],
            "value1": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class AddValue_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "AddValue", {"column_name": "value1", "value": 10}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [20, 30, 40, 50],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class MultiplyValue_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "MultiplyValue", {"column_name": "value1", "value": 10, "precision": 2}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [100, 200, 300, 400],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class DivideValue_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "DivideValue", {"column_name": "value1", "value": 10, "precision": 2}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [1.0, 2.0, 3.0, 4.0],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class AddColumns_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "AddColumns", {"column_name1": "value1", "column_name2": "value2", "inplace":True}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [20, 40, 60, 80],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class MultiplyColumns_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "MultiplyColumns", {"column_name1": "value1", "column_name2": "value2", "inplace":True, "precision": 2}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [100, 400, 900, 1600],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class DivideColumns_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "DivideColumns", {"column_name1": "value1", "column_name2": "value2", "inplace":True, "precision": 2}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [1.0, 1.0, 1.0, 1.0],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class LengthOfText_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "LengthOfText", {"column_name": "name1", "output_column": "result"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
            "result": [5, 5, 6, 12],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class SplitColumn_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "SplitColumn", {"column_name": "name1", "split_by": "_"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
            "name11": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "name12": ["Katlaa", "Rayavaram", "Vinod", "Kumar"],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class DateDiff_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "date1": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
        "date2": [datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400), datetime.fromtimestamp(500)],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "DateDiff", {"column_name1": "date1", "column_name2": "date2", "output_column": "result"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "date1": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
            "date2": [datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400), datetime.fromtimestamp(500)],
            "key1": [1, 2, 3, 4],
            "result": [100, 100, 100, 100],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class AddDate_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "date1": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
        "date2": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("SimplePipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "AddDate", {"column_name": "date1", "date": datetime.fromtimestamp(100)}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "date1": [datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400), datetime.fromtimestamp(500)],
            "date2": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
            "key1": [1, 2, 3, 4],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class IntFilter_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("Pipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "IntFilter", {"column_name": "value1", "less_than_equal_to": 30}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
            "value1": [10, 20, 30],
            "value2": [10, 20, 30],
            "key1": [1, 2, 3],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class IntegerValidation_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
        "value1": [10, 20, 30],
        "value2": [10, 20, 30],
        "key1": [1, 2, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("Pipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "IntegerValidation", {"column_name": "value1", "comparator": ">=", "value":10})

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
            "value1": [10, 20, 30],
            "value2": [10, 20, 30],
            "key1": [1, 2, 3],
        })

        assert df_expected_output.equals(output.fetch_dt("Data1").fetch_data()) == True

class StringLenValidation_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
        "value1": [10, 20, 30],
        "value2": [10, 20, 30],
        "key1": [1, 2, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("Pipeline")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "StringLenValidation", {"column_name": "name1", "comparator": ">=", "value":5})

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
            "value1": [10, 20, 30],
            "value2": [10, 20, 30],
            "key1": [1, 2, 3],
        })

        assert df_expected_output.equals(output.fetch_dt("Data1").fetch_data()) == True

class RegExValidation_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod"],
        "value1": [10, 20, 30],
        "value2": [10, 20, 30],
        "key1": [1, 2, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "RegExValidation", {"column_name": "name1", "regex": "[a-z]*_[a-z]*"})

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod"],
            "value1": [10, 20, 30],
            "value2": [10, 20, 30],
            "key1": [1, 2, 3],
        })

        assert df_expected_output.equals(output.fetch_dt("Data1").fetch_data()) == True

class CustomSql_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
        "value1": [10, 20, 30],
        "value2": [10, 20, 30],
        "key1": [1, 2, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "CustomSqlOperation", {"output_table": "data1", "sql_file": "test.sql"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "index":[0, 1, 2, 3],
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class MeanAggregate_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
        "value1": [10, 20, 30],
        "value2": [10, 20, 30],
        "key1": [1, 2, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "MeanAggregation", {"column_name": "value1"}, "Value1")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        expected_output = 20.0
        
        assert expected_output == output.fetch_dt("Value1").fetch_data()

class DateValidation_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod"],
        "value1": [10, 20, 30],
        "value2": [10, 20, 30],
        "date": ["25-06-2001", "09-06-2000", "23-05-2001"],
        "key1": [1, 2, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("Pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "DateValidation", {"column_name": "date", "format": "%d-%m-%Y"})

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod"],
            "value1": [10, 20, 30],
            "value2": [10, 20, 30],
            "date": ["25-06-2001", "09-06-2000", "23-05-2001"],
            "key1": [1, 2, 3],
        })

        assert df_expected_output.equals(output.fetch_dt("Data1").fetch_data()) == True

class SumAggregate_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
        "value1": [10, 20, 30],
        "value2": [10, 20, 30],
        "key1": [1, 2, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "SumAggregate", {"column_name": "value1"}, "Value1")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        expected_output = 60.0
        
        assert expected_output == output.fetch_dt("Value1").fetch_data()

class AddVariable_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_value1 = data.DataVariable("Value1", 60)
        input_data_dict = data.DataDict([input_table1])
        input_data_dict.edit_dt(input_value1)

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1","Value1"], "AddVariable", {"table_name": "Data1", "column_name": "value1", "variable": "Value1"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [70, 80, 90, 100],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class MultiplyVariable_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_value1 = data.DataVariable("Value1", 10)
        input_data_dict = data.DataDict([input_table1])
        input_data_dict.edit_dt(input_value1)

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1","Value1"], "MultiplyVariable", {"table_name": "Data1", "column_name": "value1", "variable": "Value1"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [100, 200, 300, 400],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class DeleteDuplicates_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar", "Jishnu_Vinod"],
            "value1": [10, 20, 30, 40, 30],
            "value2": [10, 20, 30, 40, 30],
            "key1": [1, 2, 3, 4, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "DeleteDuplicates", {}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class RegExFilter_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar", "JishnuVinod"],
            "value1": [10, 20, 30, 40, 30],
            "value2": [10, 20, 30, 40, 30],
            "key1": [1, 2, 3, 4, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "RegExFilter", {"column_name": "name1", "regex": "[a-z]*_[a-z]"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

class AppendTables_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        df2 = pd.DataFrame({
            "name1": ["rohit_katlaa"],
            "value1": [10],
            "value2": [10],
            "key1": [1],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_table2 = data.DataTable("Data2", df2)
        input_data_dict = data.DataDict([input_table1, input_table2])

        p_format = ps.PipelineFormat("pipeline1")
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1","Data2"], "AppendTables", {"input_tables": ["Data1","Data2"]}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline(input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar", "rohit_katlaa"],
            "value1": [10, 20, 30, 40, 10],
            "value2": [10, 20, 30, 40, 10],
            "key1": [1, 2, 3, 4, 1],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_data()) == True

if __name__ == "__main__":
    # Erases the data.db file
    open('data.db', 'w').close()
    LowerCase_test.test()
    UpperCase_test.test()
    AddValue_test.test()
    MultiplyValue_test.test()
    DivideValue_test.test()
    AddColumns_test.test()
    MultiplyColumns_test.test()
    DivideColumns_test.test()
    LengthOfText_test.test()
    SplitColumn_test.test()
    DateDiff_test.test()
    AddDate_test.test()
    IntFilter_test.test()
    IntegerValidation_test.test()
    StringLenValidation_test.test()
    RegExValidation_test.test()
    CustomSql_test.test()
    DateValidation_test.test()
    MeanAggregate_test.test()
    SumAggregate_test.test()
    AddVariable_test.test()
    MultiplyVariable_test.test()
    DeleteDuplicates_test.test()
    RegExFilter_test.test()
    AppendTables_test.test()



