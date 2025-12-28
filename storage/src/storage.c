#include "storage.h"
#include "operaciones.h"

int main(int argc, char* argv[]) {

    if(argc < 2){
        perror("Falta el argumento .config");
        exit(EXIT_FAILURE);
    }
      
    config = iniciar_config(argv[1]);
    logger = iniciar_logger();
    log_info(logger, "Config y logger iniciados exitosamente");

    //para el control c
    struct sigaction sa;
    sa.sa_handler = terminar_storage;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;   
    sigaction(SIGINT, &sa, NULL);

    lista_hilos = list_create();
    
    punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");

    leer_superblock();
    inicializar_fs();
    leer_bitmap();

    char* puerto = config_get_string_value(config, "PUERTO_ESCUCHA");
    
    socket_storage = iniciar_servidor(puerto);
    log_info(logger, "Storage escuchando en puerto %s", puerto);


    while (storage_activo) {
        int socket_worker = accept(socket_storage, NULL, NULL);
        if (socket_worker < 0) {
            if (!storage_activo) break;
            log_error(logger, "Error aceptando conexión de Worker");
            continue;
        }

        char* id = recibir_id(socket_worker);
        if (!id) {
            log_error(logger, "No se pudo recibir ID del Worker");
            close(socket_worker);
            continue;
        }

        //pasar a struct
        t_worker_args* args = malloc(sizeof(t_worker_args));
        args->socket = socket_worker;
        strncpy(args->id, id, sizeof(args->id)-1);
        args->id[sizeof(args->id)-1] = '\0';
        free(id);
        
        
        //+1 worker
        pthread_mutex_lock(&mutex_cant_workers);
        cantidad_workers++;
        log_info(logger, "Se conecta el Worker %s - Cantidad de Workers: %d", args->id, cantidad_workers);
        pthread_mutex_unlock(&mutex_cant_workers);

        //atender worker
        pthread_t hilo;
        pthread_create(&hilo, NULL, atender_worker, args);

        pthread_mutex_lock(&mutex_hilos);
        pthread_t* h = malloc(sizeof(pthread_t));
        *h = hilo;
        list_add(lista_hilos, h);
        pthread_mutex_unlock(&mutex_hilos);

    }

    // Esperar a todos los workers
    pthread_mutex_lock(&mutex_hilos);
    for (int i = 0; i < list_size(lista_hilos); i++) {
        pthread_t* h = list_get(lista_hilos, i);
        pthread_join(*h, NULL);
        free(h);
    }
    list_destroy(lista_hilos);
    pthread_mutex_unlock(&mutex_hilos);
    
    log_info(logger, "Cerrando Storage...");
    liberar_recursos();

    /*if (bitmap) {
        bitarray_destroy(bitmap); // libera bitmap + data
    }

    config_destroy(config);
    log_destroy(logger);*/

    return 0;
}

//WORKER
void* atender_worker(void* arg) {
    t_worker_args* wargs = (t_worker_args*)arg;
    int socket_worker = wargs->socket;

    send(socket_worker,&block_size,sizeof(int),0);
    
    char id[64];
    strncpy(id, wargs->id, sizeof(id) - 1);
    id[sizeof(id) - 1] = '\0';
    free(wargs);

    int retardo_operacion = config_get_int_value(config, "RETARDO_OPERACION");
    int retardo_acceso_bloque = config_get_int_value(config, "RETARDO_ACCESO_BLOQUE");


    while(storage_activo){
        t_list* paquete = recibir_paquete(socket_worker);
        if (!paquete) {
            log_info(logger, "No se recibieron más mensajes desde el Worker: %s.", id);
            break; // Salir del while(1) para desconectar y liberar recursos
        }

        if (list_size(paquete) == 0) {
            list_destroy_and_destroy_elements(paquete, free);
            break;
        }
        
        log_info(logger, "El paquete recibido tiene: %d elementos", list_size(paquete));
        t_storage_op operacion;
        memcpy(&operacion, list_get(paquete, 0), sizeof(t_storage_op));

        if (operacion == OP_FINISH || operacion < 0) {
            list_destroy_and_destroy_elements(paquete, free);
            break;
        }

        log_info(logger, "La operación recibida es: %d", operacion);

        usleep(retardo_operacion);

        switch (operacion) {
            case OP_CREATE_FILE: {
                char* file = list_get(paquete, 1);
                char* tag = list_get(paquete, 2);
                int tam, query_id;
                memcpy(&tam, list_get(paquete, 3), sizeof(int));
                memcpy(&query_id, list_get(paquete, 4), sizeof(int));
                
                crear_file(file, tag, query_id);

                break;
            }
            case OP_TRUNCATE_FILE: {  //cambia tamaño
                char* file = list_get(paquete, 1);
                char* tag  = list_get(paquete, 2);
                int new_size, query_id;
                memcpy(&new_size, list_get(paquete, 3), sizeof(int));
                memcpy(&query_id, list_get(paquete, 4), sizeof(int));

                truncar_file(file, tag, new_size, query_id);

                break;
            }
            case OP_WRITE_BLOCK: {  //escribe un bloque lógico (faltan manejar duplicados / bitmap)   
                char* file = list_get(paquete, 1);
                char* tag  = list_get(paquete, 2); 
                int num_block, query_id;
                memcpy(&num_block, list_get(paquete, 3), sizeof(int));
                char* data = list_get(paquete, 4);
                memcpy(&query_id, list_get(paquete, 5), sizeof(int));

                usleep(retardo_acceso_bloque); //CREO QUE FALTAN PONER EN OPERACIONES.C

                write_block(file, tag, num_block, data, query_id);

                break;
            }
            case OP_READ_BLOCK: {   //devuelve contenido de un bloque lógico    
                char* file = list_get(paquete, 1);
                char* tag  = list_get(paquete, 2);
                int num_block, query_id;
                memcpy(&num_block, list_get(paquete, 3), sizeof(int));
                memcpy(&query_id, list_get(paquete, 4), sizeof(int));

                char* data = read_block(file, tag, num_block, query_id);
                
                usleep(retardo_acceso_bloque); //CREO QUE FALTAN PONER EN OPERACIONES.C

                t_paquete* paquete_respuesta = crear_paquete();
                agregar_a_paquete(paquete_respuesta, data, block_size);
                enviar_paquete(paquete_respuesta, socket_worker); 
                eliminar_paquete(paquete_respuesta);
                
                log_info(logger, "Devolución de Read a Worker: %s", data);
                
                free(data);
                
                break;
            }
            case OP_TAG_FILE: {  //copia un Tag entero en estado WORK_IN_PROGRESS  
                char* file = list_get(paquete, 1);
                char* tag_src = list_get(paquete, 2);
                char* file_dst = list_get(paquete, 3);
                char* tag_dst = list_get(paquete, 4);
                int query_id;
                memcpy(&query_id, list_get(paquete, 5), sizeof(int));
    
                tag_file(file, tag_src, tag_dst, query_id);

                break;
            }
            case OP_COMMIT_TAG: {  //marca como COMMITED y hace deduplicación de bloques  
                char* file = list_get(paquete, 1);
                char* tag  = list_get(paquete, 2);
                int query_id;
                memcpy(&query_id, list_get(paquete, 3), sizeof(int));

                commit_tag(file, tag, query_id);

                break;
            }
            case OP_DELETE_TAG: {   //borra un Tag entero
                char* file = list_get(paquete, 1);
                char* tag  = list_get(paquete, 2);
                int query_id;
                memcpy(&query_id, list_get(paquete, 3), sizeof(int));

                delete_tag(file, tag, query_id);

                break;
            }
            case DESCONEXION:{
                pthread_mutex_lock(&mutex_cant_workers);
                cantidad_workers--;
                log_info(logger, "Se desconecta el Worker %s - Cantidad de Workers: %d", id, cantidad_workers);
                pthread_mutex_unlock(&mutex_cant_workers);
                
                close(socket_worker);
                list_destroy_and_destroy_elements(paquete, free);
                return NULL;
            }
            default:
                break;
        }

        list_destroy_and_destroy_elements(paquete, free);

    }

    //desconectar 
    //pthread_mutex_lock(&mutex_cant_workers);
    //cantidad_workers--;
    //log_info(logger, "Se desconecta el Worker %s - Cantidad de Workers: %d", id, cantidad_workers);
    //pthread_mutex_unlock(&mutex_cant_workers);

    close(socket_worker);
    return NULL;
}

void leer_superblock(){
    char* path_superblock = string_from_format("%s/superblock.config", punto_montaje);
    t_config* super = config_create(path_superblock);
    
    if (!super) {
        log_error(logger, "No se pudo abrir superblock.config");
        exit(EXIT_FAILURE);
    }

    block_size = config_get_int_value(super, "BLOCK_SIZE");
    fs_size = config_get_int_value(super, "FS_SIZE");
    
    total_blocks = fs_size / block_size;
    
    config_destroy(super);
    free(path_superblock);
}

void leer_bitmap(){
    char* path_bitmap = string_from_format("%s/bitmap.bin", punto_montaje);
    FILE* f = fopen(path_bitmap, "rb+");

    if (!f) {
        log_error(logger, "No se pudo abrir bitmap.bin");
        free(path_bitmap); 
        exit(EXIT_FAILURE);
    }

    fseek(f, 0, SEEK_END);
    size_t bitmap_size = ftell(f);
    rewind(f);

    bitmap_data = malloc(bitmap_size); //reserva tam
    fread(bitmap_data, 1, bitmap_size, f); //copia a mem
    fclose(f);

    bitmap = bitarray_create_with_mode(bitmap_data, bitmap_size, LSB_FIRST);
    
    free(path_bitmap); 
}

void liberar_recursos() {
    if (bitmap) bitarray_destroy(bitmap);
    if (bitmap_data) free(bitmap_data);
    
    if (config) config_destroy(config);
    if (logger) log_destroy(logger);

    if (socket_storage != -1) {
        shutdown(socket_storage, SHUT_RDWR);
        close(socket_storage);
    }
}


void terminar_storage(int sig) {
    if (sig == SIGINT) {
        storage_activo = 0;
        //log_info(logger, "SIGINT recibido. Cerrando Storage...");
    }
}
