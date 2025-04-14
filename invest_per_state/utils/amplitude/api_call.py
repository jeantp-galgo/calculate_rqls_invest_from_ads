import requests
import time
from datetime import datetime
import pandas as pd
import json
import re

from dotenv import load_dotenv
# Cargar variables de entorno
load_dotenv()


def get_rql_data(actual_date):
    url = 'https://amplitude.com/api/3/chart/codayant/csv'

    # Configurar cabeceras
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'User-Agent': 'PostmanRuntime/7.43.2',
        'Authorization': 'Basic ZWFiMmRlYmU5NmE2ZTRlOTkxODQ1MzgwZWVlN2IwMjQ6NjQ5NjUwYTY1NWJkMTQ0YTM2ODQ4N2M4ZjUyNDdmNmY='
    }

    print("Iniciando solicitud...")
    start_time = time.time()

    try:
        # Usar una sesión para cerrarla explícitamente después
        with requests.Session() as session:
            response = session.get(
                url, 
                headers=headers, 
                timeout=30,
                stream=True
            )
            
            print(f"Respuesta recibida en {time.time() - start_time:.2f} segundos")
            print(f"Código de estado: {response.status_code}")
            
            # Verificar si la solicitud fue exitosa
            if response.status_code == 200:
                print("Procesando respuesta...")
                actual_date = datetime.now().strftime("%Y-%m-%d")
                # Guardar la respuesta en un archivo
                with open(f'./outputs/amplitude/{actual_date}-amplitude_data.json', 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                print(f"Datos guardados en '{actual_date}-amplitude_data.json'")
                
                processed_data = process_rql_data(actual_date)
                df_clean = process_data_to_df(processed_data)
                new_df = return_df(df_clean)
                
                print(f"Datos procesados correctamente")
                print(f"Tiempo total: {time.time() - start_time:.2f} segundos")

                return new_df
                
            else:
                print(f"Error en la solicitud: {response.status_code}")
                print(f"Respuesta: {response.text}")
                return None

    except requests.exceptions.Timeout:
        print("La solicitud ha excedido el tiempo de espera (timeout)")
        return None

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {str(e)}")
        return None

    except Exception as e:
        print(f"Error general: {str(e)}")
        return None

def process_data_to_df(data):
    # Convertir a JSON y volver a cargar para eliminar los caracteres de escape
    import json

    # Convertir los datos a JSON
    json_str = json.dumps(data)

    # Cargar de nuevo desde JSON (esto debería eliminar los escapes)
    clean_data = json.loads(json_str)

    # Crear DataFrame
    df_clean = pd.DataFrame(clean_data)

    return df_clean


def return_df(df_clean):
    # Enfoque 2: Crear un nuevo DataFrame con valores explícitamente limpiados
    new_df = pd.DataFrame()

    # Para cada columna en el DataFrame original
    for col in df_clean.columns:
        # Crear nombre de columna limpio (quitar todos los caracteres \)
        clean_col_name = col
        for char in ['\\t', '\\', '\t']:
            clean_col_name = clean_col_name.replace(char, '')
        
        # Crear nueva columna con valores limpios
        if df_clean[col].dtype == 'object':  # Solo para columnas de texto
            new_values = []
            for val in df_clean[col].values:
                if isinstance(val, str):
                    # Limpiar cada valor manualmente
                    clean_val = val
                    for char in ['\\t', '\\', '\t']:
                        clean_val = clean_val.replace(char, '')
                    new_values.append(clean_val)
                else:
                    new_values.append(val)
            new_df[clean_col_name] = new_values
        else:
            # Para columnas numéricas, copiar directamente
            new_df[clean_col_name] = df_clean[col]

    return new_df

def process_rql_data(actual_date):
    try:
        # Cargar el JSON correctamente
        with open(f'./outputs/amplitude/{actual_date}-amplitude_data.json', 'r', encoding='utf-8') as archivo:
            contenido = archivo.read()
            datos_completos = json.loads(contenido)
        
        # Extraer la data
        if "data" in datos_completos:
            raw_data = datos_completos["data"]
            
            # Dividir por líneas (cada línea parece representar una fila de datos)
            lines = raw_data.strip().split('\n')
            
            # Eliminar líneas vacías y limpiar espacios
            lines = [line.strip() for line in lines if line.strip()]
            
            # Extraer encabezados (primera línea con contenido)
            headers = []
            header_line_index = 0
            for i, line in enumerate(lines):
                if "publication_code" in line and "brand" in line and "state" in line:
                    # Esta parece ser la línea de encabezados
                    # Extraer palabras entre comillas
                    headers = re.findall(r'"([^"]*)"', line)
                    header_line_index = i
                    break
            
            # Procesar filas de datos
            processed_data = []
            if headers:
                for line in lines[header_line_index+1:]:  # Empezar después de los encabezados
                    if line and not line.isspace():
                        # Extraer valores entre comillas
                        values = re.findall(r'"([^"]*)"', line)
                        
                        if values and len(values) >= len(headers):
                            # Crear diccionario con los valores
                            row_dict = {headers[j]: values[j] for j in range(len(headers))}
                            processed_data.append(row_dict)
            
            # Convertir a DataFrame si es necesario
            if processed_data:
                return processed_data
        else:
            print("No se encontró la clave 'data' en el JSON")
            return []
    except Exception as e:
        print(f"Error al procesar el archivo JSON: {str(e)}")
        return []