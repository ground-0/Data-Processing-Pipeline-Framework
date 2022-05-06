import dearpygui.dearpygui as dpg
from components.Stage import *
from components.Input import *
from components.Output import *
from lxml import etree
dpg.create_context()





class Pipeline:

    def __init__(self):

        self.input_list = []
        self.stage_list = []
        self.output_list = []
        self.namespace = "pipeline"
        self.ns = "{%s}" % self.namespace
        self.nsmap = {None:self.namespace}
        self.root = etree.Element(self.ns+"pipeline",nsmap=self.nsmap)
        self.registry = dpg.add_value_registry() 

        with dpg.window(tag="Primary Window") as window:
            
            self.inputs = dpg.add_collapsing_header(label="Pipeline Inputs", tag="input_group")
            dpg.add_button(label="add input", parent=self.inputs 
            ,callback=self.add_input_callback)
            dpg.add_separator(parent=self.inputs)
            self.area = dpg.add_collapsing_header(label="Pipeline Stages", tag="pipeline_group")
            dpg.add_button(label="add stage", parent=self.area 
            ,callback=self.add_stage_callback)
            dpg.add_separator(parent=self.area)
            self.outputs = dpg.add_collapsing_header(label="Pipeline Output", tag="output_group")
            dpg.add_button(label="add output", parent=self.outputs 
            ,callback=self.add_output_callback)
            dpg.add_separator(parent=self.outputs)


            self.pipeline_success_callback = dpg.add_string_value( parent=self.registry)
            self.pipeline_success_callback_input = dpg.add_input_text(label = "callback file", source=self.pipeline_success_callback, parent=window)

            self.pipeline_path = dpg.add_string_value( parent=self.registry)
            self.pipeline_path_input = dpg.add_input_text(label = "path", source=self.pipeline_path, parent=window)

            self.pipeline_name = dpg.add_string_value( parent=self.registry)
            self.pipeline_name_input = dpg.add_input_text(label = "pipeline name", source=self.pipeline_name, parent=window)

            dpg.add_button(label="create xml", parent=window, callback=self.create_xml_callback)

        with dpg.theme() as global_theme:

            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_color(dpg.mvThemeCol_WindowBg, (22, 22, 24), category=dpg.mvThemeCat_Core)
                dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 5, category=dpg.mvThemeCat_Core)
                dpg.add_theme_style(dpg.mvStyleVar_ItemSpacing, y=6, category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_Separator, (88, 86, 214), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_SeparatorHovered, (88, 86, 214), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_SeparatorActive, (88, 86, 214), category=dpg.mvThemeCat_Core)

            with dpg.theme_component(dpg.mvInputText):
                dpg.add_theme_color(dpg.mvThemeCol_FrameBg, (55, 55, 60), category=dpg.mvThemeCat_Core)
                dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 0, category=dpg.mvThemeCat_Core)

            with dpg.theme_component(dpg.mvButton):
                dpg.add_theme_color(dpg.mvThemeCol_Button, (66, 207, 90), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (56, 197, 80), category=dpg.mvThemeCat_Core)

            with dpg.theme_component(dpg.mvCollapsingHeader):
                dpg.add_theme_color(dpg.mvThemeCol_Header, (88, 86, 214), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_HeaderHovered, (68, 66, 195), category=dpg.mvThemeCat_Core)








        dpg.bind_theme(global_theme)


        dpg.create_viewport(title='Specification Builder', width=600, height=200)
        dpg.setup_dearpygui()
        dpg.show_viewport()
        dpg.set_primary_window("Primary Window", True)
        dpg.start_dearpygui()
        dpg.destroy_context()


    def add_stage_callback(self, sender, app_data):

        button=dpg.add_button(label="add stage", parent="pipeline_group",
        callback=self.add_stage_callback, before=sender)

        position = 0
        for stage in self.stage_list:
            
            if stage.button == sender:
                break

            position += 1
        
        self.stage_list.insert(position, Stage(parent = "pipeline_group", before = sender, button=button, parent_pipeline=self))
    
    def add_input_callback(self,sender, app_data):

        button=dpg.add_button(label="add input", parent="input_group",
        callback=self.add_input_callback, before=sender)

        position = 0
        for input in self.input_list:
            
            if input.button == sender:
                break

            position += 1
        
        self.input_list.insert(position, Input(parent = "input_group", before=sender, button=button, parent_pipeline=self))
        
    def add_output_callback(self,sender, app_data):

        button=dpg.add_button(label="add output", parent="output_group",
        callback=self.add_output_callback, before=sender)

        position = 0
        for output in self.output_list:
            
            if output.button == sender:
                break

            position += 1
        
        self.output_list.insert(position, Output(parent = "output_group", before=sender, button=button, parent_pipeline=self))
        

    def create_xml_callback(self,sender,app_data):

        self.root = etree.Element(self.ns+"specification", nsmap=self.nsmap)

        self.input_elements = etree.Element(self.ns+"inputs", nsmap=self.nsmap)
        for input in self.input_list:
            self.input_elements.append(input.create_xml())

        self.pipeline_elements = etree.Element(self.ns+"pipeline",nsmap=self.nsmap)
        for it in range(len(self.stage_list)):
            self.pipeline_elements.append(self.stage_list[it].create_xml())

        self.output_elements = etree.Element(self.ns+"outputs", nsmap=self.nsmap)
        for output in self.output_list:
            self.output_elements.append(output.create_xml())

        success = dpg.get_value(self.pipeline_success_callback_input)
        path = dpg.get_value(self.pipeline_path)

        if(path != "" and (success != "")):
            callback_element = etree.SubElement(self.root, self.ns+"callback", nsmap=self.nsmap)
            stage_path_element = etree.SubElement(callback_element, self.ns+"path", nsmap=self.nsmap)
            stage_path_element.text = path

            stage_file_element =  etree.SubElement(callback_element, self.ns+"file", nsmap=self.nsmap)
            stage_file_element.text = success

        self.root.append(self.input_elements)
        self.root.append(self.pipeline_elements)
        self.root.append(self.output_elements)

        t1 = etree.ElementTree(self.root)
        with open( dpg.get_value(self.pipeline_name)+'.xml','wb') as f:
            t1.write(f,pretty_print=True)

    def remove(self, num):

        for stage in self.stage_list:
            if stage.num == num:
                self.stage_list.remove(stage)
                break 
    
    def remove_input(self, num):

        for input in self.input_list:
            if input.num == num:
                self.input_list.remove(input)
                break
    
    def remove_output(self, num):

        for output in self.output_list:
            if output.num == num:
                self.output_list.remove(output)
                break

if __name__ == "__main__":

    Pipeline()