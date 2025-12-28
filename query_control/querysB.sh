#!/bin/bash
CONFIG="query.config"

echo ">>> INICIANDO RÁFAGA B (AGING_2 P:18 y AGING_3 P:25) cada 15s"

while true; do
    echo "[$(date +%T)] Lanzando ráfaga B..."
    ./bin/query_control "$CONFIG" "AGING_2" 18 &
    ./bin/query_control "$CONFIG" "AGING_3" 25 &
    
    sleep 15
done