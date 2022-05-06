import dearpygui.dearpygui as dpg
from components.Operation import *
from lxml import etree

class Stage:

    num=0

    def __init__(self, parent, before, button, parent_pipeline):

        self.parent = parent
        self.before = before
        self.button = button
        self.parent_pipeline = parent_pipeline

        self.num = Stage.num
        Stage.num += 1

        self.operation_list = []
        self.namespace = "pipeline"
        self.ns = "{%s}" % self.namespace
        self.nsmap = {None:self.namespace}

        self.create_ui()

    def create_ui(self):

        
        self.area = dpg.add_collapsing_header(label="stage" + str(self.num), parent=self.parent, before=self.before, indent=10)
        self.end = dpg.add_separator(parent=self.parent, before=self.before)
        self.registry = dpg.add_value_registry()      
        
        self.stage_name = dpg.add_string_value( parent=self.registry)
        self.stage_name_input = dpg.add_input_text(label = "stage name", source=self.stage_name, parent=self.area)

        # self.stage_failure_callback = dpg.add_string_value( parent=self.registry)
        # self.stage_failure_callback_input = dpg.add_input_text(label = "failure callback", source=self.stage_failure_callback, parent=self.area)

        self.stage_success_callback = dpg.add_string_value( parent=self.registry)
        self.stage_success_callback_input = dpg.add_input_text(label = "callback file", source=self.stage_success_callback, parent=self.area)

        self.stage_path = dpg.add_string_value( parent=self.registry)
        self.stage_path_input = dpg.add_input_text(label = "path", source=self.stage_path, parent=self.area)

        dpg.add_separator(parent=self.area)
        self.operations = dpg.add_group(label="ops", parent = self.area, indent=100)
        

        dpg.add_button(label="add operation", parent=self.area, callback=self.add_operation_callback)
        dpg.add_separator(parent=self.area)
        dpg.add_button(label="remove stage", parent=self.area, callback=self.remove_stage_callback)
        self.end = dpg.add_separator(parent=self.area)

        with dpg.theme() as stage_theme:

            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_color(dpg.mvThemeCol_Separator, (0, 122, 255), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_SeparatorHovered, (0, 122, 255), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_SeparatorActive, (0, 122, 255), category=dpg.mvThemeCat_Core)

            with dpg.theme_component(dpg.mvCollapsingHeader):
                dpg.add_theme_color(dpg.mvThemeCol_Header, (0, 122, 255), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_HeaderHovered, (0, 112, 235), category=dpg.mvThemeCat_Core)

        dpg.bind_item_theme(self.area, stage_theme)

        


    def create_xml(self):
        self.root = etree.Element(self.ns+"stage",nsmap=self.nsmap)
        for it in range(len(self.operation_list)):
            self.root.append(self.operation_list[it].create_xml())
        stage_name = etree.SubElement(self.root,self.ns+"stageName",nsmap=self.nsmap) 
        stage_name.text = dpg.get_value(self.stage_name_input)

        # failure = dpg.get_value(self.stage_failure_callback_input)
        success = dpg.get_value(self.stage_success_callback_input)
        path = dpg.get_value(self.stage_path)

        if(path != "" and (success != "")):

            callback_element = etree.SubElement(self.root, self.ns+"callback", nsmap=self.nsmap)
            stage_path_element = etree.SubElement(callback_element, self.ns+"path", nsmap=self.nsmap)
            stage_path_element.text = path

            stage_file_element =  etree.SubElement(callback_element, self.ns+"file", nsmap=self.nsmap)
            stage_file_element.text = success


        return self.root

    def add_operation_callback(self, sender, appdata):

        button = dpg.add_button(label="add operation", parent=self.operations, 
        callback=self.add_operation_callback, before=sender)

        position = 0
        for operation in self.operation_list:
            
            if operation.button == sender:
                break

            position += 1

        self.operation_list.insert(position, Operation(self.operations, sender, button, self))
    
    def remove_stage_callback(self, sender, appdata):

        dpg.delete_item(self.area)
        dpg.delete_item(self.button)
        dpg.delete_item(self.registry)

        self.parent_pipeline.remove(self.num)

    def remove(self, num):

        for operation in self.operation_list:
            if operation.num == num:
                self.operation_list.remove(operation)
                break

        print(self.operation_list)
