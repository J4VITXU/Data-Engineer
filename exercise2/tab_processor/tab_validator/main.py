# Importamos las bibliotecas necesarias
import os
import click
import re
import logging as log
import datetime
import shutil

INPUT_DIRECTORY = "./files/"
CLEANED_DIRECTORY = f"{INPUT_DIRECTORY}cleaned"
OUTPUT_DIRECTORY_OK = f"{INPUT_DIRECTORY}validations/ok"
OUTPUT_DIRECTORY_KO = f"{INPUT_DIRECTORY}validations/ko"
ROOT = "https://acordes.lacuerda.net"
URL_ARTIST_INDEX = "https://acordes.lacuerda.net/tabs/"
SONG_VERSION = 0
INDEX = "abcdefghijklmnopqrstuvwxyz#"


dir_list = list()
output_file = str()
dir = str()
file_name = str()


def validate_song_format(song):
    """Validates if the song follows a basic expected format."""
    # Regex pattern for song format
    pattern = r"((?:[A-Z]+\s+)*\n.+)+"

    # Check if the song matches the pattern
    match = re.fullmatch(pattern, song, flags=re.DOTALL)

    # If there is a match, the song is in the correct format
    if match:
        return True
    else:
        return False


def list_files_recursive(path: str = "."):
    """Lists all files in a directory recursively."""
    for entry in os.listdir(path):
        full_path = os.path.join(path, entry)
        if os.path.isdir(full_path):
            list_files_recursive(full_path)
        else:
            dir_list.append(full_path)

    return dir_list


@click.command()
@click.option(
    "--init",
    "-i",
    is_flag=True,
    default=False,
    help=(
        "If flag is present, drops all files and validates from the clean directory. "
    ),
)
def main(init):
    # Start time tracking
    start_time = datetime.datetime.now()
    log.info(f"Validator started at {start_time}")
    print("Starting validator...")

    if init:
        if os.path.exists(OUTPUT_DIRECTORY_OK):
            shutil.rmtree(OUTPUT_DIRECTORY_OK)
        if os.path.exists(OUTPUT_DIRECTORY_KO):
            shutil.rmtree(OUTPUT_DIRECTORY_KO)
        log.info("Directories Removed")

    OK = 0
    KO = 0

    for file_path in list_files_recursive(CLEANED_DIRECTORY):

        text = str()
        with open(file_path, "r") as file:
            text = file.read()

        # Formatting of the text goes in that function call
        validated = validate_song_format(text)

        if validated:

            output_file = file_path.replace(CLEANED_DIRECTORY, OUTPUT_DIRECTORY_OK)
            dir = "/".join(output_file.split("/")[:-1])
            file_name = output_file.split("/")[-1:]
            OK += 1
        else:

            output_file = file_path.replace(CLEANED_DIRECTORY, OUTPUT_DIRECTORY_KO)
            dir = "/".join(output_file.split("/")[:-1])
            file_name = output_file.split("/")[-1:]
            KO += 1

        # Creates the path if not exists
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
            print("OKs = ", OK, "-- KOs = ", KO, "--", dir, " CREATED!!")

        with open(output_file, "w") as file:
            file.write(text)
            print("OKs = ", OK, "-- KOs = ", KO, "--", file_name, " CREATED!!")

    log.info(f"OKs = {OK}, -- KOs = {KO}, --")
    end_time = datetime.datetime.now()
    log.info(f"Validator ended at {end_time}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")
    print(
        f"Validator finished. Duration in seconds: {duration.total_seconds()}, that is {duration.total_seconds() / 60} minutes."
    )


if __name__ == "__main__":
    main()
