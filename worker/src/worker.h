#ifndef __WORKER_H_
#define __WORKER_H_

#include<utils/utils.h>
#include<pthread.h>
#include<semaphore.h>
#include<manejo_instrucciones.h>
#include<commons/log.h>
#include<commons/config.h>

extern int conexion_storage;
extern int conexion_master;
extern void* espacio_de_usuario;
extern int tam_memoria;
extern int tam_bloque;
extern int retardo_memoria;
extern t_list* marcos;
extern t_list* marcos_libres;
extern t_list* marcos_ocupados;
extern t_list* tablas_paginas;
extern t_list* paginas_presentes;
extern t_config* config;
extern char* algoritmo_reemplazo;
extern pthread_mutex_t mutex_tablas;
extern pthread_mutex_t mutex_marcos;
extern pthread_mutex_t mutex_espacio_usuario;
extern pthread_mutex_t mutex_paginas_presentes;
extern pthread_mutex_t mutex_query_ejecutando;
extern pthread_mutex_t mutex_query_prioridad;
extern pthread_mutex_t mutex_socket_storage;
extern query_worker_h* query_en_worker;  // la query que se está ejecutando
extern query_worker_h* query_prioridad;  // la query que llegó con mayor prioridad

void inicializar_memoria_interna();
void terminar_worker();
void terminar_worker_forzado(int);
#endif

