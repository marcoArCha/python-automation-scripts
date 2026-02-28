import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OMDB_API_KEY")

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
            else:
                print(f"❌ No se encontró info para: {item}")

        except Exception as e:
            print(f"⚠️ Error de conexión: {e}")

        # Una pausa de 1 segundo para no saturar la API (buena práctica de ingeniería)
        time.sleep(1)

    print("\n--- ¡Proceso por lotes finalizado! ---")

if __name__ == "__main__":
    procesar_lote()