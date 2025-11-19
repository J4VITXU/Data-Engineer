import logging as log
import json
import sys
import utils.beautifulsoup as bs
import utils.files as files
import re
import time


from utils.data import Song, Artist
from pathlib import Path

# --- Configuration ---
ROOT = "https://acordes.lacuerda.net"
URL_ARTIST_INDEX = "https://acordes.lacuerda.net/tabs/"
SONG_VERSION = None
INDEX = "abcdefghijklmnopqrstuvwxyz"


# --- Utility Functions ---
def get_version(song, version: int = 0):
    """Get the song URL and name based on version.
    Args:
        song (str): The base song URL.
        version (int, optional): The version number. Defaults to 0.
    Returns:
        tuple: A tuple containing the modified song URL and the song name.
    """

    song = (
        str(song)
        if not version
        else str(song).replace(".shtml", f"-{str(version)}.shtml")
    )
    song_name = str(song).split("/")[-1].replace(".shtml", ".txt")

    return song, song_name


def get_artists(start_char: str, end_char: str) -> list[Artist]:
    """Scrapes artist URLs for a given range of starting letters.
    Args:
        start_char (str): The starting letter for artists to catalog (e.g., 'a').
        end_char (str): The ending letter for artists to catalog (e.g., 'z').
    Returns:
        list[Artist]: A list of Artist objects.
    """

    log.info("Starting to build artists catalog...")
    artists = []
    for char_code in range(ord(start_char), ord(end_char) + 1):
        char = chr(char_code)
        artist_index_url = f"{URL_ARTIST_INDEX}/{char}"
        log.info(f"Scraping artist index: {artist_index_url}")

        soup = bs.get_soup(artist_index_url)
        if not soup:
            continue

        ul_tag = soup.find("ul")
        if not ul_tag:
            log.info(f"No <ul> found on {artist_index_url}", file=sys.stderr)
            continue

        for li in ul_tag.find_all("li"):
            a_tag = li.find("a")
            if a_tag and a_tag.get("href"):
                href = ROOT + a_tag["href"]
                artist_display_name = Path(href).name.replace("_", " ").title()
                artists.append(Artist(name=artist_display_name, url=href))

    return artists


def get_catalog(
    output_directory: Path,
    start_char: str = "a",
    end_char: str = "z",
) -> dict:
    """
    Generates a catalog of artists and their songs from lacuerda.net.
    This function does NOT download lyrics, only metadata.
    Args:
        output_directory (Path): The base directory where lyrics would eventually be saved.
                                 Used to construct potential output_path for each song.
        start_char (str): The starting letter for artists to catalog (e.g., 'a').
        end_char (str): The ending letter for artists to catalog (e.g., 'z').
    Returns:
        dict: A dictionary with artist names as keys and lists of their Song objects as values.
    """
    start_char = start_char.lower()
    end_char = end_char.lower()

    # Get all artists
    catalog = get_artists(start_char, end_char)

    for artist in catalog:
        log.info(f"Scraping songs for artist: {artist.name} ({artist.url})")
        soup = bs.get_soup(artist.url)
        if not soup:
            continue

        for a_tag in soup.select("li > a"):
            # Filter for valid song links. lacuerda.net song links are relative
            # to the artist page and do not typically contain '.shtml' in the <a> href itself
            # for the first part of the relative path, but they *do* eventually form
            # artist/song.shtml. The original code looked for 'id="r"' which is too specific.
            # We'll assume any relative href on an artist page is a potential song link.
            if a_tag and a_tag.get("href") and not a_tag["href"].startswith("http"):

                song_relative_path = a_tag["href"]

                # Construct the full base URL for the song (before adding .shtml or version)
                # Example: https://acordes.lacuerda.net/artist/song_title
                # We need to ensure artist_url ends with a '/' if song_relative_path doesn't start with one,
                # or remove it if song_relative_path starts with one.
                if not artist.url.endswith("/") and not song_relative_path.startswith(
                    "/"
                ):
                    song_base_url_prefix = f"{artist.url}/"
                else:
                    song_base_url_prefix = artist.url

                url = f"{song_base_url_prefix}{song_relative_path}.shtml"
                full_song_url, song_filename = get_version(url, SONG_VERSION)
                song_title = (
                    Path(song_relative_path).stem.replace("_", " ").title()
                )  # The song title can be derived from the 'stem' of the relative path
                song_output_dir = f"{output_directory}songs/{artist.name.replace(' ', '_').lower()}/{song_filename}"

                artist.songs.append(
                    Song(
                        song_title=song_title,
                        song_url=full_song_url,
                        genre="",  # Cannot be scraped directly from lacuerda.net
                        lyrics_path=song_output_dir,
                    )
                )

    log.info("Cataloging complete.")
    return catalog


def get_song_lyrics(song_name: str, song_url: str, song_file_path: str) -> str:
    """Fetches the lyrics of a song from its URL.
    Args:
        song_url (str): The URL of the song page.
    Returns:
        str: The lyrics text, or an empty string if not found.
    """
    try:

        song_file_path = files.normalize_relative_path(song_file_path)

        if files.check_file_exists(song_file_path):
            log.info(f"File {song_file_path} already exists. Skipping download.")
            return False

        log.info("song --> %s - url --> %s", song_name, song_url)

        try:
            lyric = bs.get_soup(song_url).findAll("pre")
        except Exception as e:
            log.error(f"Error fetching song from {song_url}: {e}")
            return False

        for p in lyric:

            text = re.sub("<.*?>", "", str(p)).strip()
            if text:

                files.write_string_to_file(song_file_path, text=text)
                print(song_name, "downloaded!")
                return True

    except Exception as e:
        log.error(f"Error fetching lyrics from {song_url}: {e}")
        raise e


def get_songs(output_directory: str, version: int = 0):
    """
    Downloads all songs listed in catalog.json.
    Does NOT perform any scraping of artists or songs again.
    """

    catalog_path = Path(files.normalize_relative_path(f"{output_directory}catalog.json"))

    if not files.check_file_exists(catalog_path):
        log.error("catalog.json not found. Run scrapper with --update_catalog first.")
        return

    # Load catalog (list of Artist dataclasses converted to dicts)
    log.info(f"Loading catalog from {catalog_path}")
    catalog_data = files.load_from_json(catalog_path)

    # Convert dicts back to Artist / Song dataclasses (correct way)
    catalog = [Artist.from_dict(a) for a in catalog_data]

    log.info(f"Catalog loaded: {len(catalog)} artists")

    # Download each song
    for artist in catalog:
        log.info(f"Processing artist: {artist.name} ({len(artist.songs)} songs)")

        for song in artist.songs:

            # Correct versioning if needed
            song_url, song_filename = get_version(song.song_url, version)

            song_file_path = files.normalize_relative_path(song.lyrics_path)

            try:
                ok = get_song_lyrics(song_filename, song_url, song_file_path)

                if ok:
                    time.sleep(0.5)
                else:
                    log.info(f"Skipping existing file: {song_file_path}")

            except Exception as e:
                log.error(f"Error downloading {artist.name} - {song.song_title}: {e}")
                continue
