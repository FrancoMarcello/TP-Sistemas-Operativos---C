#ifndef STORAGE_H_
#define STORAGE_H_

#include "utils.h"
#include "filesystem.h"

t_log* logger;
t_config* config;
t_bitarray* bitmap;

int cantidad_workers = 0;
pthread_mutex_t mutex_cant_workers = PTHREAD_MUTEX_INITIALIZER; 
pthread_mutex_t mutex_bitmap = PTHREAD_MUTEX_INITIALIZER;

int block_size;
int fs_size;
int total_blocks;
char* punto_montaje;

void* bitmap_data;

t_list* lista_hilos;
pthread_mutex_t mutex_hilos = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int socket;
    char id[64];
} t_worker_args;

void* atender_worker(void* arg);
void leer_superblock();
void leer_bitmap();

volatile sig_atomic_t storage_activo = 1;
int socket_storage;
void liberar_recursos();
void terminar_storage(int signal);

#endif /* STORAGE_H_ */
