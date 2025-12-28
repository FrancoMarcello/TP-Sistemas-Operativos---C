#!/bin/bash
CONFIG="query.config"

echo ">>> INICIANDO RÁFAGA A (AGING_1 P:15 y AGING_2 P:20) cada 15s"

while true; do
    echo "[$(date +%T)] Lanzando ráfaga A..."
    ./bin/query_control "$CONFIG" "AGING_1" 15 &
    ./bin/query_control "$CONFIG" "AGING_2" 20 &
    
    sleep 15
done