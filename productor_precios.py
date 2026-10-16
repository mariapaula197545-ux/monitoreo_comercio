
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Conectarse al broker Kafka desde Windows
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda dato:
        json.dumps(dato).encode("utf-8")
)

TOPIC = "monitoreo-comercio"

# Productos de la tienda competidora
productos = {
    "P001": {"nombre": "Arroz", "precio": 5000},
    "P002": {"nombre": "Aceite", "precio": 9000}
}

print("Productor 1: monitoreo de precios iniciado")

try:
    while True:
        # Elegir un producto al azar
        codigo = random.choice(list(productos.keys()))
        producto = productos[codigo]

        precio_anterior = producto["precio"]

        # Simular un cambio de precio
        variacion = random.choice([-500, -200, 200, 500])
        precio_nuevo = max(500, precio_anterior + variacion)

        # Actualizar el precio conocido
        producto["precio"] = precio_nuevo

        # Crear el evento para Kafka
        evento = {
            "productor_id": "productor_1",
            "tipo_evento": "CAMBIO_PRECIO",
            "producto_id": codigo,
            "producto": producto["nombre"],
            "precio_anterior": precio_anterior,
            "precio": precio_nuevo,
            "stock": None,
            "timestamp": datetime.now().isoformat()
        }

        # Enviar el evento
        producer.send(TOPIC, value=evento).get(timeout=10)

        print(
            f"[PRODUCTOR 1] {producto['nombre']}: "
            f"${precio_anterior} -> ${precio_nuevo}"
        )

        time.sleep(3)

except KeyboardInterrupt:
    print("Productor de precios detenido")

finally:
    producer.flush()
    producer.close()
