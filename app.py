import sys
import os

# determine if application is a script file or frozen exe
if getattr(sys, 'frozen', False):
    application_path = os.path.dirname(sys.executable)
elif __file__:
    application_path = os.path.dirname(__file__)

# determine if application is a one-file exe
if hasattr(sys, "_MEIPASS"):
    # yes, resources are stored in temporary folder C:\TEMP or wherever it is
    data_path = sys._MEIPASS
else:
    # else, resources are stored in same folder as executable
    data_path = application_path
