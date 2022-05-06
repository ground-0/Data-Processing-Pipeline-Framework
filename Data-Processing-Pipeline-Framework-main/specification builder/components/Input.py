from textwrap import indent
import dearpygui.dearpygui as dpg
from lxml import etree

class Input:

    num=0

    def __init__(self, parent, before, button, parent_pipeline):

        self.parent = parent
        self.before = before
        self.button = button
        self.parent_pipeline = parent_pipeline
        self.type = "excel"

        self.num = Input.num
        Input.num += 1

        self.namespace = "pipeline"
        self.ns = "{%s}" % self.namespace
        self.nsmap = {None:self.namespace}

        self.create_ui()

    def create_ui(self):

        self.area = dpg.add_collapsing_header(label="input" + str(self.num), parent=self.parent, before=self.before, indent = 5)
        
        self.registry = dpg.add_value_registry()

        self.radio = dpg.add_radio_button(("excel", "sql", "csv"), callback=self.radio_callback, horizontal=True, parent=self.area)

        self.filter = dpg.add_filter_set(parent=self.area)
        dpg.set_value(self.filter, self.type)

        self.excel_file = dpg.add_string_value( parent=self.registry)
        self.excel_file_input = dpg.add_input_text(label = "excel file path", filter_key="excel", source=self.excel_file, parent=self.filter)

        self.csv_file = dpg.add_string_value(parent=self.registry)
        self.csv_file_input = dpg.add_input_text(label = "csv file path",  filter_key="csv", source=self.csv_file, parent=self.filter)

        self.csv_table_name = dpg.add_string_value(parent=self.registry)
        self.csv_table_name_input = dpg.add_input_text(label = "csv table name", filter_key="csv", source=self.csv_table_name, parent=self.filter)

        self.sql_host = dpg.add_string_value( parent=self.registry)
        self.sql_host_input = dpg.add_input_text(label = "sql host", filter_key="sql", source=self.sql_host, parent=self.filter)
        
        self.sql_db_name = dpg.add_string_value( parent=self.registry)
        self.sql_db_name_input = dpg.add_input_text(label = "sql database name", filter_key="sql", source=self.sql_db_name, parent=self.filter)

        self.sql_username = dpg.add_string_value( parent=self.registry)
        self.sql_username_input = dpg.add_input_text(label = "sql username", filter_key="sql", source=self.sql_username, parent=self.filter)

        self.sql_password = dpg.add_string_value( parent=self.registry)
        self.sql_password_input = dpg.add_input_text(label = "sql password", filter_key="sql", source=self.sql_password, parent=self.filter)

        self.sql_tablelist = dpg.add_string_value( parent=self.registry)
        self.sql_tablelist_input = dpg.add_input_text(label = "sql table list", filter_key="sql", source=self.sql_tablelist, parent=self.filter)

        dpg.add_button(label="remove input", parent=self.area, callback=self.delete_input_callback)
        self.end = dpg.add_separator(parent=self.area)

        with dpg.theme() as input_theme:

            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_color(dpg.mvThemeCol_Separator, (90, 200, 250), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_SeparatorHovered, (90, 200, 250), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_SeparatorActive, (90, 200, 250), category=dpg.mvThemeCat_Core)

            with dpg.theme_component(dpg.mvCollapsingHeader):
                dpg.add_theme_color(dpg.mvThemeCol_Header, (90, 200, 250), category=dpg.mvThemeCat_Core)
                dpg.add_theme_color(dpg.mvThemeCol_HeaderHovered, (80, 180, 230), category=dpg.mvThemeCat_Core)

        dpg.bind_item_theme(self.area, input_theme)


    def create_xml(self):

        self.root = etree.Element(self.ns+"input", nsmap=self.nsmap)
        self.root.attrib["type"] = self.type

        if(self.type == "sql"):
            sql_host = etree.SubElement(self.root, self.ns+"host", nsmap=self.nsmap)
            sql_host.text = dpg.get_value(self.sql_host_input)
            sql_database = etree.SubElement(self.root, self.ns+"database", nsmap=self.nsmap)
            sql_database.text = dpg.get_value(self.sql_db_name_input)
            sql_username = etree.SubElement(self.root, self.ns+"username", nsmap=self.nsmap)
            sql_username.text = dpg.get_value(self.sql_username_input)
            sql_password = etree.SubElement(self.root, self.ns+"password", nsmap=self.nsmap)
            sql_password.text = dpg.get_value(self.sql_password_input)
            sql_tables = etree.SubElement(self.root, self.ns+"tables", nsmap=self.nsmap)
            sql_tables.text = dpg.get_value(self.sql_tablelist_input)

        elif(self.type == "excel"):
            excel_file = etree.SubElement(self.root, self.ns+"file", nsmap=self.nsmap)
            excel_file.text = dpg.get_value(self.excel_file_input)

        elif(self.type == "csv"):
            csv_file = etree.SubElement(self.root, self.ns+"file", nsmap=self.nsmap)
            csv_file.text = dpg.get_value(self.csv_file_input)
            csv_table_name = etree.SubElement(self.root, self.ns+"table", nsmap=self.nsmap)
            csv_table_name.text = dpg.get_value(self.csv_table_name_input)    

        return self.root

    def delete_input_callback(self, sender, appdata):

        dpg.delete_item(self.area)
        dpg.delete_item(self.registry)
        dpg.delete_item(self.button)

        self.parent_pipeline.remove(self.num)

    def __str__(self):

        return str(self.num)

    def radio_callback(self, sender, app_data, user_data):

        self.type = app_data
        dpg.set_value(self.filter, self.type)