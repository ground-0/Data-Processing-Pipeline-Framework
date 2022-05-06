
def success_callback(stage):

    print("Custom callback says: Successfully executed {}".format(stage.name))

def failure_callback(stage, e):
    print("Custom callback says: {} execution failed with error {}".format(stage.name, e))