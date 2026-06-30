import os
import sys
import json
import logging
import redis
import requests
from datetime import datetime
from flask import Flask, request, jsonify

# =====================================================
# CONFIGURACIÓN
# =====================================================
REDIS_URL = os.environ.get('UPSTASH_REDIS_URL')
BACKEND_URL = os.environ.get('BACKEND_URL')  # Ej: http://localhost:8080/WebApplication

# Depuración
print(f"🔍 REDIS_URL = {REDIS_URL}")
print(f"🔍 BACKEND_URL = {BACKEND_URL}")

if not REDIS_URL:
    raise Exception("REDIS_URL no está configurada")
if not BACKEND_URL:
    raise Exception("BACKEND_URL no está configurada")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================
# CONEXIÓN A REDIS
# =====================================================
redis_client = None

def init_redis():
    global redis_client
    try:
        redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info(" Conectado a Upstash Redis")
    except Exception as e:
        logger.error(f" Redis error: {e}")
        sys.exit(1)

# =====================================================
# REENVÍO AL BACKEND
# =====================================================
def reenviar_al_backend(booking_id, payload, signature=None):
    """
    Reenvía el webhook al backend Java.
    """
    url = f"{BACKEND_URL}/api/v1/bokun/webhook"
    headers = {
        "Content-Type": "application/json",
        "X-Bokun-Signature": signature or "TEST_SIGNATURE"  # Si Bókun no envía firma, usamos TEST
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        if resp.status_code in (200, 201):
            logger.info(f" Webhook {booking_id} reenviado al backend. Respuesta: {resp.text}")
            return True
        else:
            logger.error(f" Error al reenviar {booking_id}: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        logger.error(f" Excepción al reenviar {booking_id}: {e}")
        return False

# =====================================================
# PROCESAMIENTO DE RESERVAS (desde la cola)
# =====================================================
def process_booking(booking_id):
    logger.info(f"🔄 Procesando booking desde cola: {booking_id}")
    
    # Recuperar el payload original de Redis (lo guardamos al recibir el webhook)
    payload_json = redis_client.get(f"webhook_payload:{booking_id}")
    if not payload_json:
        logger.error(f" No se encontró payload para {booking_id}")
        return

    payload = json.loads(payload_json)
    # Intentar extraer la firma original si existe (se puede guardar también)
    signature = redis_client.get(f"webhook_signature:{booking_id}") or "TEST_SIGNATURE"

    exito = reenviar_al_backend(booking_id, payload, signature)
    if exito:
        # Limpiar datos temporales
        redis_client.delete(f"webhook_payload:{booking_id}")
        redis_client.delete(f"webhook_signature:{booking_id}")
        logger.info(f" Webhook {booking_id} procesado y eliminado de la cola")
    else:
        # Reintentar después (se puede dejar en la cola)
        logger.warning(f" Falló reenvío de {booking_id}, se mantiene en cola para reintento")
        # Opcional: mover a cola de reintentos
        redis_client.lpush("queue:bokun:retry", booking_id)

# =====================================================
# SERVIDOR FLASK (para recibir webhooks de Bókun)
# =====================================================
app = Flask(__name__)

@app.route('/webhook/bokun', methods=['POST'])
def webhook():
    data = request.get_json()
    booking_id = data.get('bookingId')
    if not booking_id:
        return jsonify({"error": "Missing bookingId"}), 400

    event_type = request.headers.get('X-Bokun-Topic', 'unknown')
    signature = request.headers.get('X-Bokun-Signature')
    logger.info(f" Webhook recibido: {event_type} - {booking_id}")

    # Guardar payload y firma en Redis (para que el worker lo procese)
    try:
        redis_client.setex(f"webhook_payload:{booking_id}", 3600, json.dumps(data))
        if signature:
            redis_client.setex(f"webhook_signature:{booking_id}", 3600, signature)
    except Exception as e:
        logger.error(f"Error guardando en Redis: {e}")

    # Encolar en Redis (cola de trabajos)
    try:
        redis_client.lpush("queue:bokun", booking_id)
    except Exception as e:
        logger.error(f"Error encolando: {e}")

    return jsonify({"status": "ok"}), 200

@app.route('/health', methods=['GET'])
def health():
    redis_ok = False
    try:
        redis_ok = redis_client.ping()
    except:
        pass
    return jsonify({
        "redis": redis_ok,
        "status": "healthy" if redis_ok else "degraded"
    })

# =====================================================
# WORKER LOOP (consume cola de Redis)
# =====================================================
def worker_loop():
    logger.info("Worker iniciado, esperando trabajos...")
    while True:
        try:
            _, booking_id = redis_client.blpop("queue:bokun", timeout=0)
            if booking_id:
                process_booking(booking_id)
        except Exception as e:
            logger.error(f"Worker error: {e}")

# =====================================================
# PUNTO DE ENTRADA
# =====================================================
if __name__ == '__main__':
    init_redis()
    if len(sys.argv) > 1 and sys.argv[1] == 'webhook':
        # Railway asigna el puerto a través de la variable de entorno PORT
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)
    else:
        worker_loop()
