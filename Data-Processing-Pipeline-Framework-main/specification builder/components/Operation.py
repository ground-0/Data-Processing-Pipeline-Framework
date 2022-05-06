from textwrap import indent
import dearpygui.dearpygui as dpg
from lxml import etree

class Operation:

    num=0

    def __init__(self, parent, before, button, parent_stage):

        self.parent = parent
        self.before = before
        self.button = button
        self.parent_stage = parent_stage

        self.num = Operation.num
        Operation.num += 1

        self.namespace = "pipeline"
        self.ns = "{%s}" % self.namespace
        self.nsmap = {None:self.namespace}
        self.operations_lst = []
        with open('specification builder/components/operations.txt','r') as f:
            self.operations_lst = f.readlines()
        with open('specification builder/components/params.txt','r') as f:
            params_lst = f.readlines()
        self.params_dict = {}
        self.operation = ''
        for i in range(len(self.operations_lst)):
            self.params_dict[self.operations_lst[i][:-1]] = params_lst[i][:-1]
            self.operations_lst[i] = self.operations_lst[i][:-1]
        self.create_ui()
        

    def create_ui(self):

        self.area = dpg.add_collapsing_header(label="operation" + str(self.num), parent=self.parent, before=self.before, indent=10)
        self.registry = dpg.add_value_registry()

        # self.attribute = dpg.add_string_value( parent=self.registry)
        # self.attribute_input = dpg.add_input_text(label = "operation type", source=self.attribute, parent=self.area)

        self.output = dpg.add_string_value(parent=self.registry)
        self.output_input = dpg.add_input_text(label = "output", source=self.output, parent=self.area)

        self.input = dpg.add_string_value(parent=self.registry)
        self.input_input = dpg.add_input_text(label = "input", source=self.input, parent=self.area)

        # self.input_attribute = dpg.add_string_value( parent=self.registry)
        # self.input_attribute_input = dpg.add_input_text(label = "input type", source=self.input_attribute, parent=self.area)
        

        # self.name = dpg.add_string_value( parent=self.registry)
        # self.name_input = dpg.add_input_text(label = "name", source=self.name, parent=self.area)
        self.name_input = dpg.add_combo(label="operation name", items=self.operations_lst,parent=self.area,callback=self.operation_callback)

        self.parameters = dpg.add_string_value( parent=self.registry)
        self.parameters_input = dpg.add_input_text(label = "parameters", source=self.parameters, parent=self.area)

        dpg.add_button(label="remove operation", parent=self.area, callback=self.delete_operation_callback)

        self.end = dpg.add_separator(parent=self.area)

        with dpg.theme() as op_theme:

            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_color(dpg.mvThemeCol_Separator, (90, 200, 250), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_SeparatorHovered, (90, 200, 250), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_SeparatorActive, (90, 200, 250), category=dpg.mvThemeCat_Core)

            with dpg.theme_component(dpg.mvCollapsingHeader):
                dpg.add_theme_color(dpg.mvThemeCol_Header, (90, 200, 250), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_HeaderHovered, (80, 180, 230), category=dpg.mvThemeCat_Core)

        dpg.bind_item_theme(self.area, op_theme)

    def create_xml(self):
        self.root = etree.Element(self.ns+"operation",nsmap=self.nsmap)
        attrib1 = self.root.attrib
        # attrib1['type'] = dpg.get_value(self.attribute_input)
        output = etree.SubElement(self.root,self.ns+"output",nsmap=self.nsmap)
        output.text = dpg.get_value(self.output_input)
        iput = etree.SubElement(self.root,self.ns+"input",nsmap=self.nsmap)
        iput.text = dpg.get_value(self.input_input)
        attrib2 = iput.attrib
        # attrib2['type'] = dpg.get_value(self.input_attribute_input)
        name = etree.SubElement(self.root,self.ns+"name",nsmap=self.nsmap)
        name.text = dpg.get_value(self.name_input)
        parameters = etree.SubElement(self.root,self.ns+"parameters",nsmap=self.nsmap)
        parameters.text = dpg.get_value(self.parameters_input)
        return self.root
    def operation_callback(self,sender,appdata):
        self.operation = dpg.get_value(sender)
        dpg.set_value(self.parameters_input,self.params_dict[self.operation])

    def delete_operation_callback(self, sender, appdata):

        dpg.delete_item(self.area)
        dpg.delete_item(self.registry)
        dpg.delete_item(self.button)

        self.parent_stage.remove(self.num)

    def __str__(self):

        return str(self.num)