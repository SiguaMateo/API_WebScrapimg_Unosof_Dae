try:
    import pyodbc
    from dotenv import load_dotenv
    import os
except Exception as e:
    print(f"Ocurrio un error al importar las librerias de data_base, {e}")

load_dotenv()

try:
    conn = pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={os.getenv("DATABASE_SERVER")};'
        f'DATABASE={os.getenv("DATABASE_NAME")};'
        f'UID={os.getenv("DATABASE_USER")};'
        f'PWD={os.getenv("DATABASE_PASSWORD")}'
    )
    cursor = conn.cursor()
    print("Conexion realizada con exito con la Base de Datos")
except Exception as e:
    print(f"Error en la conexion con la base de datos, {e}")

# def create_table():
#     try:
#         with conn.cursor() as cursor:
#                 # Verificar si la tabla existe y eliminarla si es así
#                 cursor.execute("""
#                     IF EXISTS (SELECT * FROM sysobjects WHERE name='rptDAE_Developer' AND xtype='U')
#                     BEGIN
#                         DROP TABLE rptDAE_Developer
#                     END
#                 """)
#                 conn.commit()

#                 # Crear una nueva tabla
#                 cursor.execute("""
#                     CREATE TABLE rptDAE_Developer (
#                     id INT PRIMARY KEY IDENTITY(1,1),
#                     dae_mercado NVARCHAR(100),
#                     dae_semana INT,
#                     dae_partida_aduanera NVARCHAR(50),
#                     dae_descripcion_partida_adanuera NVARCHAR(20),
#                     dae_DAE NVARCHAR(20),
#                     dae_ruc_malima NVARCHAR(20),
#                     dae_exportador NVARCHAR(100),
#                     dae_cliente_billing NVARCHAR(100),
#                     dae_cliente_shipping NVARCHAR(100),
#                     dae_ruc_cliente_billing NVARCHAR(100),
#                     dae_pais_origen NVARCHAR(20),
#                     dae_pais_destino_billing NVARCHAR(30),
#                     dae_ciudad_destino_billing NVARCHAR(50),
#                     dae_direccion_billing NVARCHAR(200),
#                     dae_fecha_creacion_PO DATE,
#                     dae_fecha_vuelo NVARCHAR(20),
#                     dae_ID_PO NVARCHAR(20),
#                     dae_ID_customer_invoice NVARCHAR(20),
#                     dae_numero_factura_SRI NVARCHAR(20),
#                     dae_kg_bruto FLOAT,
#                     dae_kg_neto FLOAT,
#                     dae_udolares_FOB_kg FLOAT,
#                     dae_via NVARCHAR(20),
#                     dae_agente_carga NVARCHAR(50),
#                     dae_ciudad_embarque NVARCHAR(30),
#                     dae_valor_usd FLOAT,
#                     dae_especie NVARCHAR(50),
#                     dae_name_sku NVARCHAR(MAX),
#                     dae_cantidad_x_tipo_caja INT,
#                     dae_tipo_de_caja FLOAT,
#                     dae_tipos_de_caja_x_mercado NVARCHAR(30),
#                     dae_cajas_full FLOAT,
#                     dae_valor_credito FLOAT,
#                     dae_num_nota_credito_SRI NVARCHAR(20),
#                     dae_fecha_nota_credito_sri NVARCHAR(20),
#                     dae_unidad_medida NVARCHAR(20),
#                     dae_numero_tallos FLOAT,
#                     dae_numero_ramos FLOAT,
#                     dae_cantidad FLOAT,
#                     dae_FOB_unitario_x_tallos NVARCHAR(20),
#                     dae_FOB_total_restado_notas_credito NVARCHAR(20),
#                     dae_valor_incoterm_exportacion NVARCHAR(20),
#                     dae_incoterm_exportacion NVARCHAR(20),
#                     dae_periodo_promedio_cobro INT
#                     )
#                 """)
#                 conn.commit()
#     except Exception as e:
#         print(f"Ocurrio un error al crear la tabla, {e}")

# create_table()

def log_to_db(id_group, log_level, message, endpoint=None, status_code=None):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO Logs_Info (id_group, log_level, message, endpoint, status_code)
            VALUES (?, ?, ?, ?, ?)
        """, id_group, log_level, message, endpoint, status_code)
        conn.commit()

url_login_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 1 AND prm_descripcion = 'url_login'"""

user_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 1 AND prm_descripcion = 'user_name'"""

password_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 1 AND prm_descripcion = 'password'"""

user_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 5 AND prm_descripcion = 'user_mail'"""

password_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 5 AND prm_descripcion = 'password_mail'"""

url_dae_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 9 AND prm_descripcion = 'url_dae'"""

insert_query = """INSERT INTO rptDAE_Developer VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

def get_url():
    try:
        url = cursor.execute(url_login_query)
        result = cursor.fetchone()
        if result:
            url = result[0]
            print(f"URL obtenida: {url}")
            return str(url.encode('utf-8').decode('utf-8'))
        else:
            return None, "Error al obtener la url de la base de datos"
    except Exception as e:
        print(f"Ocurrio un error al obtener la url, {e}")

def get_user():
    try:
        user = cursor.execute(user_query)
        result = cursor.fetchone()
        if result:
            user = result[0]
            print(f"Usuario obtenido: {user}")
            return user.encode('utf-8').decode('utf-8')
    except Exception as e:
        print(f"Ocurrio un error al obtener el usuario, {e}")
        
def get_password():
    try:
        password = cursor.execute(password_query)
        result = cursor.fetchone()
        if result:
            password = result[0]
            print(f"Usuario obtenido: {password}")
            return password.encode('utf-8').decode('utf-8')
    except Exception as e:
        print(f"Ocurrio un error al obtener el usuario, {e}")

def get_url_dae():
    try:
        home = cursor.execute(url_dae_query)
        result = cursor.fetchone()
        if result:
            home = result[0]
            print(f"URL home obtenida: {home}")
            return home.encode('utf-8').decode('utf-8')
    except Exception as e:
        print(f"Ocurrio un error al obtener el usuario, {e}")

def get_user_mail():
    try:
        user_mail = cursor.execute(user_mail_query)
        result = cursor.fetchone()
        if result:
            user_mail = result[0]
            print(f"Usuario de correo obtenido: {user_mail}")
            return user_mail
    except Exception as e:
        print(f"Ocurrio un error al obtener el usuario de correo electronico, {e}")

def get_password_mail():
    try:
        passwd_mail =  cursor.execute(password_mail_query)
        result = cursor.fetchone()
        if result:
            passwd_mail = result[0]
            print(f"Contrasenia del correo obtenido: {passwd_mail}")
            return passwd_mail
    except Exception as e:
        print(f"Ocurrio un error al obtener la contrasenia del correo, {e}")