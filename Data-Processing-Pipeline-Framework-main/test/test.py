import dearpygui.dearpygui as dpg

dpg.create_context()

with dpg.window(label="Tutorial", pos=(20, 50), width=275, height=225) as win1:
    t1 = dpg.add_input_text(default_value="some text")
    t2 = dpg.add_input_text(default_value="some text")

    with dpg.child_window(height=-1):
        t3 = dpg.add_input_text(default_value="some text")
        dpg.add_input_int()
    dpg.add_input_text(default_value="some text")
    

with dpg.window(label="Tutorial", pos=(320, 50), width=275, height=225) as win2:
    dpg.add_input_text(default_value="some text")
    dpg.add_input_int()
    a = dpg.add_collapsing_header()
    b = dpg.add_group(parent = a)
    dpg.add_button(parent = b)
    d = dpg.add_collapsing_header()
    c = dpg.add_child_window(parent = d)
    dpg.add_button(parent = c)


with dpg.theme() as global_theme:

    with dpg.theme_component(dpg.mvAll):
        dpg.add_theme_color(dpg.mvThemeCol_FrameBg, (255, 140, 23), category=dpg.mvThemeCat_Core)
        dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 5, category=dpg.mvThemeCat_Core)

    with dpg.theme_component(dpg.mvInputInt):
        dpg.add_theme_color(dpg.mvThemeCol_FrameBg, (140, 255, 23), category=dpg.mvThemeCat_Core)
        dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 5, category=dpg.mvThemeCat_Core)

    with dpg.theme_component(dpg.mvCollapsingHeader):
        dpg.add_theme_color(dpg.mvThemeCol_ChildBg , (255, 0, 0), category=dpg.mvThemeCat_Core)

dpg.bind_theme(global_theme)

dpg.show_style_editor()


dpg.create_viewport(title='Custom Title', width=800, height=600)
dpg.setup_dearpygui()
dpg.show_viewport()
dpg.start_dearpygui()
dpg.destroy_context()