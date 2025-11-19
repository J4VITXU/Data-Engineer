import datetime
import click
import logging as log
import utils.files as files
import utils.songs as songs

# -- Configuration ---
OUTPUT_DIRECTORY = "./files/"
LOGS_DIRECTORY = "./logs/"
ROOT = "https://acordes.lacuerda.net"
URL_ARTIST_INDEX = f"{ROOT}/tabs/"
SONG_VERSION = None
INDEX = "abcdefghijklmnopqrstuvwxyz"

# --- Logging config---
logger = log.getLogger(__name__)

log.basicConfig(
    filename=f"{LOGS_DIRECTORY}scrapper.log",
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)


# --- Logic --------------------
@click.command()
@click.option(
    "-r",
    "--reset",
    is_flag=True,
    default=False,
    help="Delete the existing data and start fresh.",
)
@click.option(
    "-uc",
    "--update_catalog",
    is_flag=True,
    default=False,
    help="Regenerates the catalog.",
)
@click.option(
    "--start_char", "-sc", default="a", help="Starting letter for updating the catalog."
)
@click.option(
    "--end_char", "-ec", default="z", help="Ending letter for updating the catalog."
)
def main(reset, update_catalog, start_char, end_char):
    """Main function to run the scrapper. Can reset data, update catalog, or fetch songs."""
    print("Starting scrapper...")

    # Start time tracking
    start_time = datetime.datetime.now()
    log.info(f"Scrapper started at {start_time}")

    # Reset data if required
    if reset:
        log.info("Remove all downloaded files. Fresh start...")
        files.delete(OUTPUT_DIRECTORY)

    # Update catalog if required
    if update_catalog or not files.check_file_exists(OUTPUT_DIRECTORY, "catalog.json"):
        log.info("Updating catalog...")
        catalog = songs.get_catalog(
            OUTPUT_DIRECTORY,
            start_char=start_char,
            end_char=end_char,
        )
        files.save_to_json(catalog, OUTPUT_DIRECTORY, "catalog.json")
        log.info("Catalog updated.")

        return 200

    # Get songs lyrics
    log.info(f"Starting to download lyrics...")
    songs.get_songs(OUTPUT_DIRECTORY, version=SONG_VERSION)

    duration = datetime.datetime.now() - start_time
    log.info(f"Total duration: {duration}")
    print(f"Scrapper finished. Duration in seconds: {duration.total_seconds()}.")


if __name__ == "__main__":
    main()