#ifndef __MASTER_H_
#define __MASTER_H_

#include<utils/utils.h>
#include<pthread.h>
#include<semaphore.h>

typedef enum {
    LECTURA,
    FIN,
    DESALOJO,
    DESCONEXION_WORKER,
    DESCONEXION_QUERY,
    EJECUTAR_QUERY
} t_master_op;

typedef enum {
    WORKER_LIBRE,
    DESALOJO_QUERY,
    DESALOJO_PASIVO
} t_worker_op;

//void controlar_querys();
void atender_querys(int conexion);
void* manejar_querys(void* arg);
int recibir_id_worker(int conexion);
int recibir_handshake(int conexion);
query* crear_query(int conexion, int id, char* archivo, int prioridad);
worker* crear_worker(int, int, query*);
bool comparar_queries(void* q_a, void* q_b);
void manejar_worker(void* worker);
void destruir_worker(void*);
void destruir_query(void*);
void terminar_master(int);
worker* buscar_worker_con_query(int id_query);
bool es_worker_ocupado(int id_buscar);
bool es_worker_libre(int id_buscar);
query* buscar_query_por_id(int id_buscar);
query* buscar_query_ready_por_id(int id_buscar);
query* buscar_query_fin_por_id(int id_buscar);
int query_a_desalojar(int prioridad);
void* aging(void* arg);
void* aging_individual(void* arg);
query* revisar_desalojo_por_aging();

#endif