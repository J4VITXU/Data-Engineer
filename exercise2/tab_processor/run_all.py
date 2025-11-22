import subprocess
import logging as log
from pathlib import Path

LOG_FILE = Path("./logs/pipeline.log")

log.basicConfig(
    filename=str(LOG_FILE),
    filemode="w",
    encoding="utf-8",
    level=log.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)

MODULES = [
    "scrapper/main.py",
    "tab_cleaner/main.py",
    "tab_validator/main.py",
    "lyrics.py",
    "insights.py",
    "results.py",
]

def run_module(module_path):
    try:
        log.info(f"Running module: {module_path}")
        subprocess.run(["python", module_path], check=True)
        log.info(f"Completed: {module_path}")
    except Exception as e:
        log.error(f"Failed: {module_path} | Error: {e}")


def main():
    for module in MODULES:
        run_module(f"tab_processor/{module}")


if __name__ == "__main__":
    main()
