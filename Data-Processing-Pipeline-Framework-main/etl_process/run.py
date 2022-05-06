import sys, os
sys.path.append((os.path.join(os.getcwd(), "pipeline extecution")))

import main

main.DPPF_run("./etl_process/etl.xml")