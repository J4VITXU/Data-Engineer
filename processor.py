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
# Carpeta donde se guardarán los archivos descargados y extraídos
DOWNLOAD_DIR = "downloads"

# Función para crear la carpeta de descargas si no existe
def create_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)  # Crea la carpeta; si ya existe, no da error
    logging.info(f"Carpeta '{DOWNLOAD_DIR}' creada o ya existente.")

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
        logging.info(f"Archivo descargado: {filename}")  # Mensaje de confirmación

        # Abrir el ZIP y extraer su contenido
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)  # Extrae todo en la carpeta de descargas
        logging.info(f"Archivo extraído: {filename}")  # Mensaje de confirmación

        os.remove(filepath)  # Elimina el archivo ZIP original
        logging.info(f"Archivo ZIP eliminado: {filename}")  # Mensaje de confirmación

    # Manejo de errores en caso de problemas de descarga
    except requests.exceptions.RequestException as e:
        logging.error(f"No se pudo descargar {uri}: {e}")  # Mensaje si falla la descarga
    # Manejo de errores si el archivo ZIP está corrupto
    except zipfile.BadZipFile:
        logging.error(f"Archivo ZIP corrupto: {filename}")  # Mensaje si el ZIP no se puede abrir
    # Captura cualquier otro error
    except Exception as e:
        logging.error(f"Error procesando {filename}: {e}")  # Mensaje de error genérico

# Función principal que ejecuta todo el proceso
def main() -> None:
    logging.basicConfig(level=logging.INFO)  # Configura logging para mostrar mensajes de info y error
    create_download_dir()  # Crear carpeta de descargas

    # Iterar sobre cada URL y descargar + extraer
    for uri in download_uris:
        download_and_extract(uri)  # Llama a la función para procesar cada archivo

    logging.info("Proceso finalizado.")  # Mensaje al final del proceso

# Ejecuta la función principal solo si se ejecuta este archivo directamente
if __name__ == "__main__":
    main()