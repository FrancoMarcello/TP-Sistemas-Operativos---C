#!/bin/bash
echo ">>> INICIANDO IMPARES ÚNICOS (1 -> 3 -> 5)"

# Definimos los pares de (config, id)
CONFIGS=("worker1.config" "worker3.config" "worker5.config")
IDS=(1 3 5)

# 1. Lanzamos el primero (ID 1)
./bin/worker ${CONFIGS[0]} ${IDS[0]} &
PID_VIEJO=$!
echo "[$(date +%T)] Worker ${IDS[0]} conectado (PID: $PID_VIEJO)"

# 2. Iteramos para conectar el siguiente y matar al anterior
for i in {1..2}; do
    sleep 10
    ./bin/worker ${CONFIGS[$i]} ${IDS[$i]} &
    PID_NUEVO=$!
    echo "[$(date +%T)] Worker ${IDS[$i]} conectado (PID: $PID_NUEVO)"

    sleep 10
    echo "[$(date +%T)] Desconectando para siempre al Worker antiguo (PID: $PID_VIEJO)..."
    kill -SIGINT $PID_VIEJO 2>/dev/null
    
    PID_VIEJO=$PID_NUEVO
done

# 3. Esperamos un tiempo final y matamos al último
sleep 10
echo "[$(date +%T)] Finalizando último worker impar (PID: $PID_VIEJO)..."
kill -SIGINT $PID_VIEJO 2>/dev/null
echo ">>> Script de Impares finalizado. No se repitieron IDs."