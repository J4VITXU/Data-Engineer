import os
import re
import logging as log
import datetime
from pathlib import Path

from utils.string_mapping import MAPPING

# -- Configuration ---
INPUT_DIRECTORY = "./files/"
LOGS_DIRECTORY = "./logs/"

OUTPUT_DIRECTORY = f"{INPUT_DIRECTORY}cleaned/"
MIN_LINES = 5

# --- Logging config---
logger = log.getLogger(__name__)

log.basicConfig(
    filename=f"{LOGS_DIRECTORY}cleaner.log",
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)

# --- Utility ---


def list_files_recursive(path="."):
    """Return list of all files under path (recursively)."""
    files = []
    for root, dirs, filenames in os.walk(path):
        for name in filenames:
            files.append(os.path.join(root, name))
    return files


def remove_email_sentences(text: str):
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    sentence_pattern = r"[\n^.!?]*" + email_pattern + r"[^.!?]*[.!?\n]"
    return re.sub(sentence_pattern, "", text)


def apply_format_rules(text: str):
    formatted_text = remove_email_sentences(text)

    for key, value in MAPPING.items():
        formatted_text = re.sub(
            key, value, formatted_text, flags=re.DOTALL | re.IGNORECASE
        )
    return formatted_text


def main():
    # Start time tracking
    start_time = datetime.datetime.now()
    log.info(f"Cleaner started at {start_time}")
    print("Starting cleaner...")

    cleaned = 0

    # ✅ Solo limpiamos las TABS, no todo ./files/
    songs_dir = os.path.join(INPUT_DIRECTORY, "songs")

    for file_path in list_files_recursive(songs_dir):
        log.info(f"Processing file -> {file_path}")

        # Leer SIEMPRE en UTF-8 y tolerante (evita UnicodeDecodeError)
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
        except Exception as e:
            log.error(f"Error reading {file_path}: {e}")
            continue

        if text.count("\n") < MIN_LINES:
            log.info("Empty or too small tab. Skipping.............................")
            continue

        # Aplicar reglas de limpieza
        formatted_text = apply_format_rules(text)

        # ✅ Mantener estructura relativa desde ./files/songs/
        relative_path = os.path.relpath(file_path, songs_dir)
        output_file = os.path.join(OUTPUT_DIRECTORY, relative_path)

        dir_path = os.path.dirname(output_file)

        # Creates the path if not exists
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            print("INFO", dir_path, " CREATED!!")

        cleaned += 1
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(formatted_text)
            print(cleaned, "--", [os.path.basename(output_file)], " CREATED!!")

    end_time = datetime.datetime.now()
    log.info(f"Cleaner ended at {end_time}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")
    print(
        f"Cleaner finished. Duration in seconds: {duration.total_seconds()}, "
        f"that is {duration.total_seconds() / 60} minutes."
    )


if __name__ == "__main__":
    main()
