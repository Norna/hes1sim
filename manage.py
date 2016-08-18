import os
import sys
from molnframework.core import serialisers

if __name__ == "__main__":

    """ Just Keep It """

    os.environ.setdefault("MOLNFRAMEWORK_SETTINGS_MODULE","settings")
    from molnframework.core.management import execute_command_line
    execute_command_line(sys.argv)