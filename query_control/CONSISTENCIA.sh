CONFIG="query.config"
NOMBRES=("AGING_1" "AGING_2") # Solo dos por ráfaga para reducir el choque inicial
INSTANCIAS=50 # 50 ciclos * 2 queries/ciclo = 100 queries
TIEMPO_ENTRE_CICLOS="0.100" # 100,000 microsegundos = 0.100 segundos

echo ">>> LANZAMIENTO CONTROLADO EN 5 SEGUNDOS."
echo ">>> Intervalo: ${TIEMPO_ENTRE_CICLOS}s entre ráfagas de 2 queries."

for ((i=1; i<=INSTANCIAS; i++)); do
    echo "------ ITERACIÓN $i / 50 ------"
    
    # Ráfaga de 2 queries lanzadas casi al mismo tiempo (microsegundos entre ellas)
    for NOMBRE in "${NOMBRES[@]}"; do
        # Ajusta la prioridad si es necesario, aquí usamos 20
        nice -n 20 ./bin/query_control "$CONFIG" "$NOMBRE" 20 & 
    done
    
    # Espera 100 milisegundos (0.100 segundos) para regular el ritmo
    sleep "$TIEMPO_ENTRE_CICLOS"
done

echo ">>> Todas las 100 queries fueron lanzadas."CONFIG="query.config"
NOMBRES=("AGING_1" "AGING_2") # Solo dos por ráfaga para reducir el choque inicial
INSTANCIAS=50 # 50 ciclos * 2 queries/ciclo = 100 queries
TIEMPO_ENTRE_CICLOS="0.100" # 100,000 microsegundos = 0.100 segundos

echo ">>> LANZAMIENTO CONTROLADO EN 5 SEGUNDOS."
echo ">>> Intervalo: ${TIEMPO_ENTRE_CICLOS}s entre ráfagas de 2 queries."

for ((i=1; i<=INSTANCIAS; i++)); do
    echo "------ ITERACIÓN $i / 50 ------"
    
    # Ráfaga de 2 queries lanzadas casi al mismo tiempo (microsegundos entre ellas)
    for NOMBRE in "${NOMBRES[@]}"; do
        # Ajusta la prioridad si es necesario, aquí usamos 20
        nice -n 20 ./bin/query_control "$CONFIG" "$NOMBRE" 20 & 
    done
    
    # Espera 100 milisegundos (0.100 segundos) para regular el ritmo
    sleep "$TIEMPO_ENTRE_CICLOS"
done

echo ">>> Todas las 100 queries fueron lanzadas."