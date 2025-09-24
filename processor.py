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
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)  # Crea la carpeta; si ya existe, no da error
    print(f"Carpeta '{DOWNLOAD_DIR}' creada o ya existente.")

# Función para descargar un archivo ZIP, extraerlo y eliminar el ZIP
def download_and_extract(uri: str):
    filename = uri.split("/")[-1]  # Extrae solo el nombre del archivo de la URL
    filepath = os.path.join(DOWNLOAD_DIR, filename)  # Construye la ruta completa del archivo

    try:
        response = requests.get(uri, stream=True)  # Descarga el archivo en streaming
        response.raise_for_status()  # Lanza error si la URL no es válida o falla la descarga

        # Guardar el archivo ZIP en disco
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):  # Descarga por partes
                f.write(chunk)  # Escribe cada parte en el archivo
        print(f"Archivo descargado: {filename}")

        # Abrir el ZIP y extraer su contenido
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)  # Extrae todo en la carpeta de descargas
        print(f"Archivo extraído: {filename}")

        os.remove(filepath)  # Elimina el archivo ZIP original
        print(f"Archivo ZIP eliminado: {filename}")  # Mensaje de confirmación

    # Manejo de errores
    except requests.exceptions.RequestException as e:
        print(f"No se pudo descargar {uri}: {e}")  # Mensaje si falla la descarga
    except zipfile.BadZipFile:
        print(f"Archivo ZIP corrupto: {filename}")  # Mensaje si el ZIP no se puede abrir

def main() -> None:
    create_download_dir()

    for uri in download_uris:
        download_and_extract(uri)

    print("Proceso finalizado.")

if __name__ == "__main__":
    main()