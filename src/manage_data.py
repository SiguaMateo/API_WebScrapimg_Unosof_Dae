try:
    import csv
    from src import data_base
    from src import send_mail
    from datetime import datetime, timedelta
except Exception as e:
    print(f"Ocurrio un error al importar las liberias en manage data, {e}")

delete_query = """DELETE FROM dbo.rptDAE_Developer
            WHERE dae_fecha_creacion_PO > ?"""

def delete_old_records():
    today = datetime.now()
    date_calculate = (today - timedelta(days=15)).date()
    try:
        data_base.cursor.execute(delete_query, date_calculate)
        data_base.cursor.commit()
        print("La eliminacion se completo ... ")
    except Exception as e:
        print(f"Error al eliminar los registros anteriores, {e}")

def save():
    delete_old_records()
    try:
        with open("unosof_data.csv", mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)

            # Ignorar el encabezado
            next(reader, None)

            for row in reader:
                print("Ingreso al bucle")
                print(f"Fila leida: {row}")

                data_base.cursor.execute(data_base.insert_query, row)
                print(f"Datos guardados correctamente")

        data_base.cursor.commit()        

    except Exception as e:
        message = f"Ocurrio un error al guardar los datos en la base de datos, {e}"
        print(message)
        data_base.log_to_db(1, "ERROR", message, endpoint='fallido', status_code=404)
        send_mail.send_mail(message)