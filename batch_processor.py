import requests
import time
import os
from dotenv import load_dotenv
import csv
import datetime

load_dotenv()
API_KEY = os.getenv("OMDB_API_KEY")

def registrar_error(item, motivo):
    archivo_errores = 'errores.log'
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(archivo_errores, mode='a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] ERROR: No se procesó '{item}'. Motivo: {motivo}\n")

def guardar_en_csv(datos_pelicula):
    archivo_csv = 'reporte_peliculas.csv'
    # Definimos los encabezados (las columnas de tu "base de datos")
    columnas = ['Titulo', 'Año', 'Director', 'Rating', 'Genero']
    
    # Comprobamos si el archivo ya existe para no escribir la cabecera dos veces
    archivo_existe = os.path.isfile(archivo_csv)
    
    with open(archivo_csv, mode='a', newline='', encoding='utf-8') as archivo:
        writer = csv.DictWriter(archivo, fieldnames=columnas)
        
        if not archivo_existe:
            writer.writeheader() # Escribe Titulo, Año, etc. solo la primera vez
        
        writer.writerow(datos_pelicula)

def procesar_lote():
    # 1. LEER EL ARCHIVO (Como si fueran tus IDs de Jira)
    try:
        with open('list.txt', 'r') as archivo:
            lineas = archivo.readlines()
    except FileNotFoundError:
        print("Error: No encontré el archivo lista.txt")
        return

    print(f"--- Se encontraron {len(lineas)} elementos para procesar ---")

    # 2. BUCLE DE AUTOMATIZACIÓN (El corazón del Data Engineering)
    for linea in lineas:
        item = linea.strip() # Limpiamos espacios o saltos de línea
        if not item: continue

        print(f"\nProcesando: {item}...")
        
        # Simulamos la consulta a la API (usando la de películas que vimos antes)
        url = f"https://www.omdbapi.com/?t={item}&apikey={API_KEY}"
        
        try:
            response = requests.get(url)
            data = response.json()

            if data.get("Response") == "True":
                print(f"✅ Datos recuperados: {data.get('Title')} ({data.get('Year')})")
                # Aquí es donde en el futuro enviarías la data al SDI o a tu DB
                if data.get("Response") == "True":
                    pelicula_limpia = {
                        'Titulo': data.get('Title'),
                        'Año': data.get('Year'),
                        'Director': data.get('Director'),
                        'Rating': data.get('imdbRating'),
                        'Genero': data.get('Genre')
                }
                guardar_en_csv(pelicula_limpia)
                print(f"✅ {pelicula_limpia['Titulo']} guardada en el reporte.")
            else:
                print(f"❌ No se encontró info para: {item}")
                # EL NUEVO MANEJO DE ERRORES:
                motivo_error = data.get("Error", "Desconocido")
                print(f"❌ Falló: {item} ({motivo_error})")
                registrar_error(item, motivo_error)

        except Exception as e:
            print(f"⚠️ Error de conexión: {e}")

        # Una pausa de 1 segundo para no saturar la API (buena práctica de ingeniería)
        time.sleep(1)

    print("\n--- ¡Proceso por lotes finalizado! ---")

if __name__ == "__main__":
    procesar_lote()