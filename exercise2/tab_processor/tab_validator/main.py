import re
import logging as log
import datetime
import shutil
from pathlib import Path

import click

# --- Configuration ---
INPUT_DIRECTORY = Path("./files")
CLEANED_DIRECTORY = INPUT_DIRECTORY / "cleaned"
OUTPUT_DIRECTORY_OK = INPUT_DIRECTORY / "validations" / "ok"
OUTPUT_DIRECTORY_KO = INPUT_DIRECTORY / "validations" / "ko"

LOGS_DIRECTORY = Path("./logs")

ROOT = "https://acordes.lacuerda.net"
URL_ARTIST_INDEX = "https://acordes.lacuerda.net/tabs/"

# --- Logging config ---
logger = log.getLogger(__name__)

log.basicConfig(
    filename=str(LOGS_DIRECTORY / "validator.log"),
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)

# --- Validation rules ---

# Regla original: formato básico
BASE_PATTERN = r"((?:[A-Z]+\s+)*\n.+)+"

# ✅ Regla nueva: debe haber al menos un acorde tipo A, Am, C#, Fm, etc.
CHORD_PATTERN = re.compile(r"\b([A-G][#b]?m?(maj7)?(sus2|sus4)?7?)\b")


def validate_song_format(song: str) -> bool:
    """
    Validates if the song follows a basic expected format
    AND contains at least one chord pattern.
    """

    # Regla 1: formato básico (la que ya tenías)
    match = re.fullmatch(BASE_PATTERN, song, flags=re.DOTALL)
    if not match:
        return False

    # ✅ Regla 2 (nueva): al menos un acorde reconocible
    if not CHORD_PATTERN.search(song):
        return False

    return True


def iter_cleaned_files():
    """Itera por todos los .txt en el directorio cleaned."""
    for path in CLEANED_DIRECTORY.rglob("*.txt"):
        if path.is_file():
            yield path


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

    # Limpiar salidas si se usa --init
    if init:
        if OUTPUT_DIRECTORY_OK.exists():
            shutil.rmtree(OUTPUT_DIRECTORY_OK)
        if OUTPUT_DIRECTORY_KO.exists():
            shutil.rmtree(OUTPUT_DIRECTORY_KO)
        log.info("Validation output directories removed")

    OK = 0
    KO = 0

    # Nos aseguramos de que existen las carpetas raíz
    OUTPUT_DIRECTORY_OK.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIRECTORY_KO.mkdir(parents=True, exist_ok=True)

    for file_path in iter_cleaned_files():
        # Leer siempre en UTF-8 y tolerante
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            log.error(f"Error reading {file_path}: {e}")
            KO += 1
            # En caso de error de lectura, lo tratamos como KO
            rel = file_path.relative_to(CLEANED_DIRECTORY)
            output_file = OUTPUT_DIRECTORY_KO / rel
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text("", encoding="utf-8")
            continue

        validated = validate_song_format(text)

        # Construimos la ruta de salida manteniendo la estructura relativa
        rel = file_path.relative_to(CLEANED_DIRECTORY)
        if validated:
            output_file = OUTPUT_DIRECTORY_OK / rel
            OK += 1
        else:
            output_file = OUTPUT_DIRECTORY_KO / rel
            KO += 1

        # Creamos la carpeta si no existe
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Guardamos el fichero en la ruta correspondiente
        output_file.write_text(text, encoding="utf-8")

        print(
            "OKs = ",
            OK,
            "-- KOs = ",
            KO,
            "--",
            [output_file.name],
            " CREATED!!",
        )

    log.info(f"OKs = {OK}, -- KOs = {KO}, --")
    end_time = datetime.datetime.now()
    log.info(f"Validator ended at {end_time}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")
    print(
        f"Validator finished. Duration in seconds: {duration.total_seconds()}, "
        f"that is {duration.total_seconds() / 60} minutes."
    )


if __name__ == "__main__":
    main()
