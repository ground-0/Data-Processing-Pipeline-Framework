from email import message
from lxml import etree
import pipeline_structure
import ast
import data
import sys, os


class Specification:
	def __init__(self, specificationFile : str) -> None:
		self.et = etree.parse(specificationFile)
		self.schemaFile = "./test/pipeline.xsd"
		self.ns = "{pipeline}"
		self.name = specificationFile.split('.')[0]

	def validate(self) -> str:
		schemaET = etree.parse(self.schemaFile)
		schema = etree.XMLSchema(schemaET)
		root = self.et.getroot()
		# print(root[0].getparent().tag)
		# for element in root.iter():
		# 	print("%s - %s" % (element.tag,element.text))
		# 	print(element.keys())
		result = schema.validate(self.et)
		
		if(result):
			return "valid"
		else:
			errors = ""
			log = schema.error_log
			for error in log:
				errors += "Error line {row}: {message}\n".format(row=error.line, message=error.message)
			return errors
	
	def getTaskFormat(self, taskRoot : etree._Element):
		output_name = ""
		input_name=""
		operation_name=""
		operation_params={}
		getOutputName = False
		getInputName=False
		getOperationName=""
		getOperationParameter=False
		try:
			for i in range(len(taskRoot)):
				if(taskRoot[i].tag==self.ns + "output"):
					if(not getOutputName):
						getOutputName = True
						output_name = taskRoot[i].text
				if(taskRoot[i].tag== self.ns + "input"):
					if(not getInputName):
						getInputName = True
						try:
							input_name = ast.literal_eval(taskRoot[i].text)
						except:
							input_name = [taskRoot[i].text]
				if(taskRoot[i].tag== self.ns + "name" or taskRoot[i].tag=="{pipeline}formula"):
					if(not getOperationName):
						getOperationName = True
						operation_name = taskRoot[i].text
				if(taskRoot[i].tag== self.ns + "parameters"):
					if(not getOperationParameter):
						operation_params = ast.literal_eval(taskRoot[i].text)
			x = pipeline_structure.TaskFormat(input_name, operation_name, operation_params, output_name)
		except Exception as e:
			raise Exception("Task creation failed with error " + str(e))
		else:
			return x

	def getStageFormat(self,stageRoot : etree._Element):
		stageFormat = pipeline_structure.StageFormat("")
		for i in range(len(stageRoot)):
			if(stageRoot[i].tag== self.ns + "operation"):
				try:
					t = self.getTaskFormat(stageRoot[i])
				except Exception as e:
					raise Exception("Stage creation failed with error " + str(e))
				else:
					stageFormat.add_task_format(t)
			if(stageRoot[i].tag== self.ns + "stageName"):
				stageFormat.name = stageRoot[i].text
			if(stageRoot[i].tag== self.ns + "callback"):
				
				path = stageRoot[i][0].text 

				sys.path.append(os.path.join(os.path.dirname(sys.path[0]),path))
				callbacks = __import__(stageRoot[i][1].text)

				if(hasattr(callbacks, "failure_callback")):
					stageFormat.set_failure_callback(callbacks.failure_callback)
				if(hasattr(callbacks, "success_callback")):
					stageFormat.set_success_callback(callbacks.success_callback)
				

		return stageFormat

	def getPipelineFormat(self):
		self.validate()
		root = self.et.getroot()[1]
		pipelineFormat = pipeline_structure.PipelineFormat(self.name)
		
		for i in root:

			if(i.tag ==  self.ns + "stage"):
				pipelineFormat.add_stage_format(self.getStageFormat(i))
			elif(i.tag ==  self.ns + "callback"):
				callbacks = __import__(i.text)
				
				if(hasattr(callbacks, "failure_callback")):
					pipelineFormat.set_failure_callback(callbacks.failure_callback)
				if(hasattr(callbacks, "success_callback")):
					pipelineFormat.set_success_callback(callbacks.success_callback)
		
		return pipelineFormat

	def getInputDict(self):

		inputList = []
		root = self.et.getroot()[0]
		
		for j in root:

			if j.attrib['type'] == 'sql':

				for i in j:

					if(i.tag == self.ns + "database"):
						database = i.text
					elif(i.tag == self.ns + "username"):
						username = i.text
					elif(i.tag == self.ns + "password"):
						password = i.text
					elif(i.tag == self.ns + "tables"):
						tables = ast.literal_eval( i.text )
					elif(i.tag == self.ns + "host"):
						host = i.text
				
				dict = {'host':host, 'db_name':database, 'username':username, 'password':password, 'table_name_list':tables}	

				inputList.append(("sql", dict))

			elif j.attrib['type'] == 'csv':

				for i in j:

					if(i.tag == self.ns + "file"):
						file_path = i.text
					elif(i.tag == self.ns + "table"):
						table_name = i.text

				dict = {"file_path":file_path,"table_name":table_name}
				inputList.append(("csv", dict))

			elif j.attrib['type'] == 'excel':

				for i in j:

					if(i.tag == self.ns + "file"):
						file_path = i.text

				dict = {"file_path":file_path}
				inputList.append(("excel", dict ))

		return data.DataReader.generate_data_dict(inputList)

	def getOutputDict(self):

		outputList = []
		root = self.et.getroot()[2]

		for j in root:

			if j.attrib['type'] == 'sql':

				for i in j:

					if(i.tag == self.ns + "database"):
						database = i.text
					elif(i.tag == self.ns + "username"):
						username = i.text
					elif(i.tag == self.ns + "password"):
						password = i.text
					elif(i.tag == self.ns + "tables"):
						tables = ast.literal_eval( i.text )
					elif(i.tag == self.ns + "host"):
						host = i.text	

				
				dict = {'host':host, 'db_name':database, 'username':username, 'password':password, 'tables':tables}	
				outputList.append(("sql", dict))

			elif j.attrib['type'] == 'csv':

				for i in j:

					if(i.tag == self.ns + "file"):
						file_path = i.text
					elif(i.tag == self.ns + "table"):
						table_name = i.text

				dict = {"file_path":file_path,"table_name":table_name}
				outputList.append(("csv", dict))

			elif j.attrib['type'] == 'excel':

				for i in j:

					if(i.tag == self.ns + "file"):
						file_path = i.text
					elif(i.tag == self.ns + "tables"):
						tables = ast.literal_eval( i.text )


				dict = {"file_path":file_path, "tables": tables}
				outputList.append(("excel", dict ))

		return outputList

if __name__ == "__main__":
	sp = Specification("./specification builder/pipeline1.xml")
	print(sp.validate())
	pipeline_format = sp.getPipelineFormat()
	p = pipeline_structure.Pipeline()
