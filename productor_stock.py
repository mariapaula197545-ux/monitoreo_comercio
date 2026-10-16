
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Conectarse al broker Kafka
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda dato:
        json.dumps(dato).encode("utf-8")
)

TOPIC = "monitoreo-comercio"

# Inventario inicial de otros productos
inventario = {
    "P003": {"nombre": "Leche", "stock": 8},
    "P004": {"nombre": "Huevos", "stock": 6}
}

print("Productor 2: monitoreo de inventario iniciado")

try:
    while True:
        codigo = random.choice(list(inventario.keys()))
        producto = inventario[codigo]

        stock_anterior = producto["stock"]

        # Disminuir las unidades disponibles
        if stock_anterior > 0:
            disminucion = random.randint(1, 2)
            stock_nuevo = max(0, stock_anterior - disminucion)
        else:
            # Reponer para continuar demostrando el sistema
            stock_nuevo = random.randint(5, 8)

        producto["stock"] = stock_nuevo

        # Crear el evento de inventario
        evento = {
            "productor_id": "productor_2",
            "tipo_evento": "CAMBIO_STOCK",
            "producto_id": codigo,
            "producto": producto["nombre"],
            "precio_anterior": None,
            "precio": None,
            "stock_anterior": stock_anterior,
            "stock": stock_nuevo,
            "timestamp": datetime.now().isoformat()
        }

        # Enviar el evento a Kafka
        producer.send(TOPIC, value=evento).get(timeout=10)

        if stock_nuevo == 0:
            estado = "PRODUCTO AGOTADO"
        elif stock_nuevo <= 3:
            estado = "ALERTA DE STOCK BAJO"
        else:
            estado = "STOCK NORMAL"

        print(
            f"[PRODUCTOR 2] {producto['nombre']}: "
            f"{stock_anterior} -> {stock_nuevo} unidades | {estado}"
        )

        time.sleep(2)

except KeyboardInterrupt:
    print("Productor de inventario detenido")

finally:
    producer.flush()
    producer.close()
