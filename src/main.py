try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.select import Select
    from bs4 import BeautifulSoup
    from datetime import datetime, timedelta, date
    from src import data_base
    from src import send_mail
    import time
    import pandas as pd 
    import os
except Exception as e:
    print(f"Error al importar las librerias en main, {e}")

def login():
    max_retries = 2
    retries = 0
    global driver
    while retries < max_retries:
        try:
            # Inicializar el navegador
            chrome_options = Options()
            chrome_options.binary_location = "/usr/bin/google-chrome"
            # chrome_options.add_argument("--headless") # visualizar el navegador
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            driver.get(data_base.get_url())

            # Iniciar sesión
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "username"))).send_keys(data_base.get_user())
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "password"))).send_keys(data_base.get_password())

            message = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "Login"))).click()
            print(message)

            time.sleep(5)  # Esperar un poco
            break  # Si el login fue exitoso, salimos del bucle

        except Exception as e:
            retries += 1
            print(f"Ocurrio un error al iniciar sesion, {e}")
            time.sleep(3)
            if retries == max_retries:
                data_base.log_to_db(1, "ERROR", f"Ocurrio un error al iniciar sesión, {e}", endpoint='fallido', status_code=500)
                send_mail.send_mail(f"Ocurrio un error al inciar sesión, {e}")
                raise
            else:
                # Solo continuar si no alcanzamos el máximo de reintentos
                time.sleep(5)
    
def scroll_down():
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

# Obtencion de la fecha actual
hoy = date.today()
fechahoy=datetime.now()
fechaFInal=hoy - timedelta(3)
fechaActual = hoy.strftime("%m/%d/%Y")
fechaResta=fechaFInal.strftime("%m/%d/%Y")
fechasql=fechaFInal.strftime("%Y-%m-%d")

def scraple_data():
    max_retries = 5
    retries = 0
    login()

    start_date = datetime(2019, 1, 1)  # Fecha de inicio
    end_date = datetime.today()  # Fecha de hoy

    # Aseguramos que las fechas estén bien definidas
    if start_date > end_date:
        print("La fecha de inicio no puede ser mayor que la fecha de hoy.")
        return

    while retries < max_retries:
        try:
            driver.get("https://malimamaster.unosof.com/index.cfm?event=FUES.Reports")

            # Iniciar sesión
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "username"))).send_keys(data_base.get_user())
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "password"))).send_keys(data_base.get_password())
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "Login"))).click()

            driver.get("https://malimamaster.unosof.com/index.cfm?event=FUES.Reports")

            # Iterar cada 15 días desde la fecha de inicio hasta la fecha de hoy
            while start_date <= end_date:
                print(f"Extrayendo datos del rango de fechas: {start_date} a {start_date + timedelta(days=14)}")
                
                # Convertir las fechas de inicio y fin a strings
                start_date_str = start_date.strftime('%Y-%m-%d')
                end_date_str = (start_date + timedelta(days=14)).strftime('%Y-%m-%d')

                driver.get("https://malimamaster.unosof.com/index.cfm?event=FUES.Reports")

                # Rellenar el formulario con las fechas
                fechaInicio = driver.find_element(By.NAME, 'dt_search_start_1')
                fechaInicio.clear()
                fechaInicio.send_keys(start_date_str)
                fechaFin = driver.find_element(By.NAME, 'dt_search_end_1')
                fechaFin.clear()
                fechaFin.send_keys(end_date_str)
                time.sleep(1)

                # Seleccionar los filtros
                select = Select(driver.find_element(By.ID, 'dt_filter_1'))
                select.select_by_index(2)

                select = Select(driver.find_element(By.ID, 'reportID1'))
                select.select_by_index(26)

                btnGenerarReporte = driver.find_element(By.NAME, 'GenerateReport_1')
                btnGenerarReporte.click()
                time.sleep(50)

                scroll_down()

                contenidoPagina = driver.page_source
                soup = BeautifulSoup(contenidoPagina, "html.parser")

                rows = []
                for a in soup.find(id='tblAWBDetail').find_all("td", {"class": "noclass"}):
                    rows.append(a.text)

                rango = len(rows) // 44  # Calcular cuántas filas de datos hay

                # Definir las cabeceras de las columnas
                headers = [f"Column{i+1}" for i in range(44)]  # Personalizar según el contenido

                rows_data = []
                for x in range(int(rango)):
                    row = rows[x * 44:(x + 1) * 44]
                    if len(row) == 44:  # Verificar que la fila tiene el número correcto de columnas
                        # Limpiar las celdas
                        row = [cell.replace('\n', ' ').strip() for cell in row]

                        # Unir direcciones fragmentadas si es necesario
                        if len(row[12].split()) > 2:  # Verificar si es una dirección fragmentada
                            row[12] = " ".join(row[12].split())
                            
                        # Procesar la fecha (columna 14)
                        try:
                            dateTimeObj = datetime.strptime(row[14], '%b-%d-%Y')
                            row[14] = dateTimeObj
                        except Exception as e:
                            print(f"Error al convertir la fecha: {e}")
                            row[14] = None

                        # Añadir la fila a los datos
                        rows_data.append(row)
                    else:
                        print(f"Fila omitida debido a un número incorrecto de columnas: {len(row)}")

                # Crear DataFrame y guardar los datos
                if rows_data:
                    df = pd.DataFrame(rows_data, columns=headers)
                    file_path = os.path.join(os.path.dirname(__file__), 'unosof_data.csv')

                    # Si el archivo ya existe, añadir los datos a continuación
                    if os.path.exists(file_path):
                        df.to_csv(file_path, mode='a', header=False, index=False)
                    else:
                        df.to_csv(file_path, index=False)
                    print(f"Datos guardados correctamente en '{file_path}'.")

                else:
                    print("No se encontraron filas con el número esperado de columnas.")

                # Actualizar la fecha de inicio para la próxima iteración (15 días después)
                start_date += timedelta(days=15)

            break

        except Exception as e:
            message = f"Error al realizar el web scraping: {e}"
            retries += 1
            time.sleep(5)
            data_base.log_to_db(1, message, endpoint='fallido', status_code=404)

        finally:
            driver.quit()
            driver.close()

            if retries >= max_retries:
                print("Máximo de reintentos alcanzado. Finalizando proceso.")