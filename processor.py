import os
import logging
import requests
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip"
]

DOWNLOAD_DIR = "downloads"

def create_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    logging.info(f"Carpeta '{DOWNLOAD_DIR}' creada o ya existente.")

def download_and_extract(uri: str):
    filename = uri.split("/")[-1]
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        response = requests.get(uri, stream=True)
        response.raise_for_status()
        
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Archivo descargado: {filename}")
        
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        logging.info(f"Archivo extraído: {filename}")
        
        os.remove(filepath)
        logging.info(f"Archivo ZIP eliminado: {filename}")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"No se pudo descargar {uri}: {e}")
    except zipfile.BadZipFile:
        logging.error(f"Archivo ZIP corrupto: {filename}")
    except Exception as e:
        logging.error(f"Error procesando {filename}: {e}")

def main() -> None:
    logging.basicConfig(level=logging.INFO)
    create_download_dir()
    
    for uri in download_uris:
        download_and_extract(uri)
    
    logging.info("Proceso finalizado.")

if __name__ == "__main__":
    main()
