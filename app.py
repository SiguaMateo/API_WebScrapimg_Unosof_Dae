try:
    from fastapi import FastAPI
    from src import main, manage_data
    import uvicorn
except Exception as e:
    print(f"Error al importar las librerias en main.py, {e}")

app = FastAPI(
    title="API WebScraping DAE Unosof",
    description="API para la extraccion de datos de la pagina de Unosof",
    version="1.0.0"
)

@app.get("/", description="Endpoint raiz")
def default_endpoint():
    return { " message " : " Inicio la API WebScrapinf DAE Unosof "}

@app.get("/get-data-unosof", description="Endpoint para extraer la data de las pagina unosof")
def get_data():
    try:
        main.scraple_data()
    except Exception as e:
        print(f"Error al realizar el webscraping, {e}")

@app.get("/save")
def save_data():
    manage_data.save()

if __name__ == "__main__":
    uvicorn.run(app=app, host="0.0.0.0", port=9994)