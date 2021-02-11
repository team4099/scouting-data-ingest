import sys

from rich.console import Console
from loguru import logger

console = Console()

class Formatter:
    def __init__(self):
        self.padding = 0
        self.fmt = "<b><light-green>{time:MM-DD hh:mm:ss:SS}</light-green> | <level>{level: <8}</level> | {module: <14}:{line: <4} |</b> {message}\n"

    def format(self, record):
        mod = record['module']
        color = ""
        if mod == 'DataManager':
            color = "blue"
        elif mod == "DataInput":
            color = "yellow"
        elif mod == "DataProcessor":
            color = "light-magenta"
        elif mod == "DataCalculator":
            color = "cyan"
        elif mod == "DataAccessor":
            color = "light-green"

        return self.fmt.replace('{module: <14}',f"<{color}>"+"{module: <14}"+f"</{color}>")

logger.remove(0)
f = Formatter()
logger.add(sys.stdout, colorize=True, format=f.format)
logger.level("DATA", no=39, color="<red><d>")