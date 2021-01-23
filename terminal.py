from rich.console import Console
import logging
from rich.logging import RichHandler

console = Console()

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(markup=True)],
)
logger = logging.getLogger("rich")
