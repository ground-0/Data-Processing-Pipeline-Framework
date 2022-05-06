import data
import operations
from typing import Dict, List, Callable


OPERATIONS_MAPPING = {
  "RenameColumn": operations.RenameColumnOperation,
  "DeleteColumn": operations.DeleteColumnOperation,
  "LowerCase": operations.LowerCaseOperation,
  "UpperCase": operations.UpperCaseOperation,
  "AddValue": operations.AddValueOperation,
  "MultiplyValue": operations.MultiplyValueOperation,
  "DivideValue": operations.DivideValueOperation,
  "AddColumns": operations.AddColumnsOperation,
  "MultiplyColumns": operations.MultiplyColumnsOperation,
  "DivideColumns": operations.DivideColumnsOperation,
  "LengthOfText": operations.LengthOfTextOperation,
  "DateDiff": operations.DateDiffOperation,
  "AddDate": operations.AddDateOperation,
  "SplitColumn": operations.SplitColumnOperation,
  "JoinTableOperation": operations.JoinTableOperation,
  "IntegerValidation": operations.IntegerValidation,
  "IntFilter": operations.IntFilterOperation,
  "StringLenValidation": operations.StringLenValidation,
  "RegExValidation":operations.RegExValidation,
  "CustomSqlOperation":operations.CustomSqlOperation,
  "DateValidation":operations.DateValidation,
  "CustomFilter": operations.CustomFilterOperation,
  "CustomTransform": operations.CustomTransformOperation,
  "CustomValidation": operations.CustomValidation,
  "MeanAggregation": operations.MeanAggregationOperation,
  "AddVariable": operations.AddVariableOperation,
  "SubVariable": operations.SubVariableOperation,
  "MultiplyVariable":operations.MultiplyVariableOperation,
  "MedianAggregate":operations.MedianAggregateOperation,
  "ModeAggregate":operations.ModeAggregateOperation,
  "SumAggregate":operations.SumAggregateOperation,
  "StdAggregate":operations.StdAggregateOperation,
  "CountAggregate":operations.CountAggregateOperation,
  "MinAggregate":operations.MinAggregateOperation,
  "MaxAggregate":operations.MaxAggregateOperation,
  "DateValidation":operations.DateValidation,
  "DeleteDuplicates":operations.DeleteDuplicatesOperation,
  "RegExFilter":operations.RegExFilter,
  "AppendTables":operations.AppendTablesOperation,
  "LowerCaseValidation":operations.LowerCaseValidation,
  "InsertColumn":operations.InsertColumnOperation,
  "UpperCaseValidation":operations.UpperCaseValidation,
  "UniqueColumnValidation":operations.UniqueColumnValidation,
}
POSSIBLE_OPERATIONS = OPERATIONS_MAPPING.keys()


class DefaultCallback:

  @staticmethod
  def stage_success_callback(stage):
    print("Successfully executed {}".format(stage.name))

  @staticmethod
  def stage_failure_callback(stage, e):
    print("{} execution failed with error {}".format(stage.name, e))

  @staticmethod
  def pipeline_success_callback(pipeline):
    print("Successfully executed {}".format(pipeline.name))

  @staticmethod
  def pipeline_failure_callback(pipeline, e):
    print("{} execution failed with error {}".format(pipeline.name, e))


class TaskFormat:
  
  def __init__(self, input_names: List[str], operation_name: str, operation_params: dict, output_name: str =None):
    self.input_names = input_names
    if not operation_name in POSSIBLE_OPERATIONS:
      raise Exception("Operation not available")
    self.operation_name = operation_name
    self.operation_params = operation_params
    self.output_name = output_name


class StageFormat:
  
  def __init__(self, name: str):
    self.name = name
    self.tasks:List[TaskFormat] = []
    self.success_callback = DefaultCallback.stage_success_callback
    self.failure_callback = DefaultCallback.stage_failure_callback

  def add_task_format(self, task_format: TaskFormat):
    self.tasks.append(task_format)

  def set_success_callback(self, callback: Callable):
    self.success_callback = callback

  def set_failure_callback(self, callback: Callable):
    self.failure_callback = callback

  def get_success_callback(self) -> Callable:
    return self.success_callback

  def get_failure_callback(self) -> Callable:
    return self.failure_callback


class PipelineFormat:
  
  def __init__(self, name):
    self.name = name
    self.stages:Dict[str, StageFormat] = {}
    self.success_callback = DefaultCallback.pipeline_success_callback
    self.failure_callback = DefaultCallback.pipeline_failure_callback

  def add_stage_format(self, stage_format: StageFormat):
    self.stages[stage_format.name] = stage_format

  def fetch_stage_format(self, name: str):
    return self.stages[name]

  def set_success_callback(self, callback: Callable):
    self.success_callback = callback

  def set_failure_callback(self, callback: Callable):
    self.failure_callback = callback

  def get_success_callback(self) -> Callable:
    return self.success_callback

  def get_failure_callback(self) -> Callable:
    return self.failure_callback


class Task:
  
  def __init__(self, operation: operations.BaseOperation, task_format: TaskFormat):
    self.input_names = task_format.input_names
    self.operation = operation
    if not self.operation.validate_params(task_format.operation_params):
      raise Exception("Invalid params")
    self.operation_params = task_format.operation_params
    self.output_name = task_format.output_name

  def get_task_details(self):
    return "{} {}".format(self.operation.get_name(), self.operation.get_type())

  def run(self, dd: data.DataDict) -> data.DataDict:
    if self.operation.get_type() == "SimpleValidation":
      input = dd.fetch_dt(self.input_names[0])
      if self.operation.run(self.operation_params, input, self.output_name):
        return dd
      else:
        raise Exception("{} failed.".format(self.get_task_details()))

    if self.operation.get_type() == "SimpleOperation" or self.operation.get_type() == "AggregateOperation":
      input = dd.fetch_dt(self.input_names[0])
    elif self.operation.get_type() == "ComplexOperation":
      input = dd
    else:
      raise Exception("Invalid operation type")

    dt = self.operation.run(self.operation_params, input, self.output_name)
    dd.edit_dt(dt)
    return dd


class Stage:

  def __init__(self, stage_format: StageFormat):
    self.name = stage_format.name
    self.stage_format = stage_format
    self.task_list:List[Task] = []
    self.success_callback = None
    self.failure_callback = None
    self.construct(stage_format)

  def set_success_callback(self, callback: Callable):
    self.success_callback = callback

  def set_failure_callback(self, callback: Callable):
    self.failure_callback = callback

  def get_success_callback(self) -> Callable:
    return self.success_callback

  def get_failure_callback(self) -> Callable:
    return self.failure_callback

  def get_stage_format(self) -> StageFormat:
    return self.stage_format

  def construct(self, stage_format: StageFormat):
    for task_format in stage_format.tasks:
      task = Task(OPERATIONS_MAPPING[task_format.operation_name], task_format)
      self.task_list.append(task)

  def execute(self, data_dict: data.DataDict) -> data.DataDict:
    for task in self.task_list:
      data_dict = task.run(data_dict)
    return data_dict


class Pipeline:

  def __init__(self, input_data_dict: data.DataDict, pipeline_format: PipelineFormat):
    self.name = pipeline_format.name
    self.data_dict = input_data_dict
    self.stage_list:List[Stage] = []
    self.pipeline_format = pipeline_format
    self.success_callback = pipeline_format.get_success_callback()
    self.failure_callback = pipeline_format.get_failure_callback()
    self.construct(pipeline_format)
  
  def construct(self, pipeline_format: PipelineFormat):
    for stage_name in pipeline_format.stages.keys():
      stage_format = pipeline_format.fetch_stage_format(stage_name)
      stage = Stage(stage_format)
      stage.set_success_callback(stage_format.get_success_callback())
      stage.set_failure_callback(stage_format.get_failure_callback())
      self.stage_list.append(stage)

  def execute(self) -> data.DataDict:
    try:
      for stage in self.stage_list:
        try:
          self.data_dict = stage.execute(self.data_dict)
          stage.get_success_callback()(stage)
        except Exception as e:
          stage.get_failure_callback()(stage, e)
          raise e
      self.success_callback(self)
      return self.data_dict
    except Exception as e:
      self.failure_callback(self, e)
      return self.data_dict

  def clean_up(self):
    self.data_dict.clean_up()
