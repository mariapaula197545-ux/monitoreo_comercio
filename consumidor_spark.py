
import json
import mysql.connector

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)

# 1. Crear la sesión de Spark y habilitar el uso de los núcleos disponibles
# local[*] permite que Spark utilice varios núcleos del computador para procesar datos.
spark = (
    SparkSession.builder
    .appName("MonitoreoPreciosYStock")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("Consumidor Spark iniciado")


# 2. Definir la estructura de los eventos recibidos desde Kafka
# Se especifican los campos y sus tipos para interpretar correctamente los mensajes JSON.
esquema = StructType([
    StructField("productor_id", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("producto", StringType(), True),
    StructField("precio_anterior", DoubleType(), True),
    StructField("precio", DoubleType(), True),
    StructField("stock_anterior", IntegerType(), True),
    StructField("stock", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])


# 3. Leer en tiempo real los eventos publicados en el tema de Kafka
eventos_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "monitoreo-comercio")
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir los mensajes binarios de Kafka a texto JSON y separar sus campos.
eventos = (
    eventos_kafka
    .selectExpr("CAST(value AS STRING) AS mensaje")
    .select(from_json(col("mensaje"), esquema).alias("dato"))
    .select("dato.*")
)


# 4. Analizar los eventos y generar las alertas correspondientes
eventos_procesados = (
    eventos
    .withColumn(
        "alerta",
        # Comparar precios y cantidades para identificar cambios importantes.
        when(
            (col("tipo_evento") == "CAMBIO_PRECIO")
            & (col("precio") < col("precio_anterior")),
            lit("BAJADA_PRECIO")
        )
        .when(
            (col("tipo_evento") == "CAMBIO_PRECIO")
            & (col("precio") > col("precio_anterior")),
            lit("SUBIDA_PRECIO")
        )
        .when(
            (col("tipo_evento") == "CAMBIO_STOCK")
            & (col("stock") == 0),
            lit("PRODUCTO_AGOTADO")
        )
        .when(
            (col("tipo_evento") == "CAMBIO_STOCK")
            & (col("stock") <= 3),
            lit("STOCK_BAJO")
        )
        .otherwise(lit("OK"))
    )
    # Registrar cuándo Spark procesa cada evento.
    .withColumn("procesado_en", current_timestamp())
)


# 5. Guardar en MySQL los eventos procesados por Spark
def guardar_lote(lote_df, id_lote):

    # Ignorar los microlotes que no contienen eventos.
    if lote_df.isEmpty():
        return

    # Obtener las filas del microlote para guardarlas en MySQL.
    filas = lote_df.collect()

    conexion = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="rescue123",
        database="monitoreo_comercio"
    )

    cursor = conexion.cursor()

    try:
        # Crear la tabla de eventos si todavía no existe.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS eventos (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                lote_id BIGINT NOT NULL,
                productor_id VARCHAR(50),
                tipo_evento VARCHAR(50),
                producto_id VARCHAR(50),
                producto VARCHAR(100),
                precio_anterior DOUBLE NULL,
                precio DOUBLE NULL,
                stock_anterior INT NULL,
                stock INT NULL,
                timestamp_evento VARCHAR(40),
                alerta VARCHAR(50),
                procesado_en DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Preparar la consulta para insertar los eventos en MySQL.
        sql = """
            INSERT INTO eventos (
                lote_id, productor_id, tipo_evento, producto_id,
                producto, precio_anterior, precio, stock_anterior,
                stock, timestamp_evento, alerta
            )
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
        """

        for fila in filas:
            datos = fila.asDict()

            # Mostrar en la terminal el productor, el producto y la alerta detectada.
            print(
                f"[LOTE {id_lote}] "
                f"[{datos['productor_id']}] "
                f"{datos['producto']} | "
                f"Alerta: {datos['alerta']}"
            )

            # Insertar los datos del evento y su alerta en la base de datos.
            cursor.execute(sql, (
                id_lote,
                datos["productor_id"],
                datos["tipo_evento"],
                datos["producto_id"],
                datos["producto"],
                datos["precio_anterior"],
                datos["precio"],
                datos["stock_anterior"],
                datos["stock"],
                datos["timestamp"],
                datos["alerta"]
            ))

        conexion.commit()
        print(f"Lote {id_lote} guardado correctamente: {len(filas)} eventos")

    except Exception:
        # Deshacer los cambios si ocurre un error durante la inserción.
        conexion.rollback()
        raise

    finally:
        cursor.close()
        conexion.close()


# 6. Ejecutar el procesamiento continuo mediante microlotes
consulta = (
    eventos_procesados.writeStream
    # Enviar cada microlote a la función que almacena los datos en MySQL.
    .foreachBatch(guardar_lote)
    # Guardar información de recuperación del procesamiento.
    .option(
        "checkpointLocation",
        "./checkpoints/monitoreo-comercio"
    )
    # Intentar procesar los eventos en intervalos de cinco segundos.
    .trigger(processingTime="5 seconds")
    .start()
)

print("Escuchando eventos de precios e inventario...")

try:
    # Mantener Spark activo mientras recibe y procesa los eventos.
    consulta.awaitTermination()
except KeyboardInterrupt:
    print("Deteniendo consumidor Spark...")
    consulta.stop()
finally:
    # Cerrar Spark cuando finalice el programa.
    spark.stop()
