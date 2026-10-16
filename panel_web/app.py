
import os
from datetime import date, datetime
from decimal import Decimal

import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

# Crear la aplicación web
app = FastAPI(title="Monitoreo de Precios y Stock")

# Configuración de conexión a MySQL.
# La contraseña se obtiene de una variable de entorno.
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "monitoreo_comercio"),
}

# Conectarse a MySQL y consultar información.
def consultar_mysql(sql, parametros=None):
    conexion = None
    cursor = None

    try:
        conexion = mysql.connector.connect(**DB_CONFIG)
        cursor = conexion.cursor(dictionary=True)
        cursor.execute(sql, parametros or ())
        return cursor.fetchall()

    except mysql.connector.Error as error:
        print("Error de conexión o consulta MySQL:", error)
        raise HTTPException(
            status_code=500,
            detail="No se pudo consultar MySQL. Revisa la conexión y las credenciales."
        )

    finally:
        if cursor:
            cursor.close()
        if conexion and conexion.is_connected():
            conexion.close()


# Página principal: comprobar conexión con la base de datos.
@app.get("/api/estado")
def estado():
    filas = consultar_mysql("SELECT DATABASE() AS base_datos")
    return {"estado": "conectado", "base_datos": filas[0]["base_datos"]}


# Métricas principales del panel.
@app.get("/api/metricas")
def metricas():
    sql = """
        SELECT
            COUNT(*) AS total_eventos,
            COALESCE(SUM(alerta = 'SUBIDA_PRECIO'), 0) AS subidas_precio,
            COALESCE(SUM(alerta = 'BAJADA_PRECIO'), 0) AS bajadas_precio,
            COALESCE(SUM(alerta = 'STOCK_BAJO'), 0) AS stock_bajo,
            COALESCE(SUM(alerta = 'PRODUCTO_AGOTADO'), 0) AS productos_agotados
        FROM eventos
    """
    filas = consultar_mysql(sql)
    return JSONResponse(content=jsonable_encoder(filas[0]))


# Mostrar el evento más reciente de cada producto.
@app.get("/api/productos")
def productos():
    sql = """
        SELECT
            e.producto_id,
            e.producto,
            e.precio_anterior,
            e.precio,
            e.stock_anterior,
            e.stock,
            e.alerta,
            e.tipo_evento,
            e.timestamp_evento
        FROM eventos e
        INNER JOIN (
            SELECT producto_id, MAX(id) AS ultimo_id
            FROM eventos
            GROUP BY producto_id
        ) ultimos ON e.id = ultimos.ultimo_id
        ORDER BY e.producto ASC
    """
    filas = consultar_mysql(sql)
    return JSONResponse(content=jsonable_encoder(filas))


# Mostrar los eventos más recientes registrados por Spark.
@app.get("/api/eventos")
def eventos():
    sql = """
        SELECT
            id,
            lote_id,
            productor_id,
            tipo_evento,
            producto_id,
            producto,
            precio_anterior,
            precio,
            stock_anterior,
            stock,
            timestamp_evento,
            alerta,
            procesado_en
        FROM eventos
        ORDER BY id DESC
        LIMIT 25
    """
    filas = consultar_mysql(sql)
    return JSONResponse(content=jsonable_encoder(filas))


# Publicar los archivos del panel web.
app.mount(
    "/",
    StaticFiles(directory="panel_web/static", html=True),
    name="panel"
)
