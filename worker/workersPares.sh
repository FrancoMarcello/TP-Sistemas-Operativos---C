#!/bin/bash
echo ">>> INICIANDO PARES ÚNICOS (2 -> 4 -> 6)"

CONFIGS=("worker2.config" "worker4.config" "worker6.config")
IDS=(2 4 6)

# 1. Lanzamos el primero (ID 2)
./bin/worker ${CONFIGS[0]} ${IDS[0]} &
PID_VIEJO=$!
echo "[$(date +%T)] Worker ${IDS[0]} conectado (PID: $PID_VIEJO)"

# 2. Iteramos
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

# 3. Final
sleep 10
echo "[$(date +%T)] Finalizando último worker par (PID: $PID_VIEJO)..."
kill -SIGINT $PID_VIEJO 2>/dev/null
echo ">>> Script de Pares finalizado. No se repitieron IDs."