#include "master.h"
#include <sys/time.h>
int master_fd;
t_list* workers;
t_list* querys;
t_list* workers_libres;
t_list* workers_ocupados;
t_list* querys_activas;
t_list* querys_fin;
int ultimo_id_query = -1;
sem_t sem_workers_libres;
pthread_mutex_t mutex_workers;
pthread_mutex_t mutex_workers_libres;
pthread_mutex_t mutex_workers_ocupados;
pthread_mutex_t mutex_querys_activas;
pthread_mutex_t mutex_querys;
pthread_mutex_t mutex_querys_fin;
pthread_mutex_t mutex_id_query;
char* planificador;
int tiempo_aging;

int main(int argc, char* argv[]) {

    if(argc < 2){
        printf("Falta el argumento .config");
        return EXIT_FAILURE;
    }

    config = config_create(argv[1]);
    logger = log_create("master.log","MASTER",1,log_level_from_string(config_get_string_value(config,"LOG_LEVEL")));
    workers = list_create();
    querys = list_create();
    workers_libres = list_create();
    workers_ocupados = list_create();
    querys_activas = list_create();
    querys_fin = list_create();
    sem_init(&sem_workers_libres, 0,0);
    pthread_mutex_init(&mutex_workers, NULL);
    pthread_mutex_init(&mutex_workers_libres, NULL);
    pthread_mutex_init(&mutex_workers_ocupados, NULL);
    pthread_mutex_init(&mutex_querys, NULL);
    pthread_mutex_init(&mutex_querys_activas, NULL);
    pthread_mutex_init(&mutex_querys_fin, NULL);
    pthread_mutex_init(&mutex_id_query,NULL);

    signal(SIGINT, terminar_master); //Ctrl+C
    signal(SIGHUP, terminar_master); //Cierre de terminal.

    char* puerto_master = config_get_string_value(config, "PUERTO_ESCUCHA");
    master_fd = iniciar_servidor(puerto_master);
    planificador = config_get_string_value(config,"ALGORITMO_PLANIFICACION");
    tiempo_aging = config_get_int_value(config,"TIEMPO_AGING");

    while(1){
        int conexion = esperar_cliente(master_fd); //Recibe un Worker o una Query
        int handshake = recibir_handshake(conexion);

        if(conexion < 0){
            log_warning(logger, "Conexion detectada fallida");
            continue;
        }

        if(handshake < 20 && handshake > 0){    //Si el id es 20, lo va a tomar como query
        int worker_id = handshake;
        worker* nuevo_worker;
        nuevo_worker = crear_worker(conexion, worker_id, NULL);
        pthread_mutex_lock(&mutex_workers);
        list_add(workers, nuevo_worker); // todos los workers presentes
        int cantidad_workers = list_size(workers);
        pthread_mutex_unlock(&mutex_workers);
        log_info(logger, "## Se conecta el Worker %d - Cantidad total de Workers: %d", worker_id, cantidad_workers);

        pthread_mutex_lock(&mutex_workers_libres);
        list_add(workers_libres,nuevo_worker);
        pthread_mutex_unlock(&mutex_workers_libres);
        sem_post(&sem_workers_libres);

        pthread_t t_worker;
        pthread_create(&t_worker, NULL, (void*) manejar_worker, (void*)nuevo_worker);
        pthread_detach(t_worker);

        } else if(handshake == 20){
            pthread_mutex_lock(&mutex_id_query);
            int id_iterado = ++ultimo_id_query;
            int bytes = send(conexion,&id_iterado,sizeof(int), 0);
            if( bytes <= 0){
                log_error(logger, "No se pudo enviar el ID a la query");
                pthread_mutex_unlock(&mutex_id_query);                
                close(conexion);
                continue;
            }
            pthread_mutex_unlock(&mutex_id_query);

        pthread_t h_query;
        pthread_create(&h_query,NULL,(void*) manejar_querys, (void*)(intptr_t) conexion);
        pthread_detach(h_query);
        }
    }

    return 0;
}

void* manejar_querys(void* arg){
    int conexion = (intptr_t)arg;
    while(1){
        t_list* query_conectada = recibir_paquete(conexion);

        if (!query_conectada || query_conectada == NULL || list_size(query_conectada) <=0) {
            //log_warning(logger, "La Query se desconecto. Cerrando hilo.");
            //log_info(logger, "## Se desconecta un Query Control. Se finaliza la Query <%d> con prioridad <%d>. Nivel multiprocesamiento <%d>",query_conectada->id,query_conectada->prioridad,list_size(workers_ocupados));
            close(conexion);
            pthread_exit(NULL);
            return NULL;
        }

        t_master_op op = *(t_master_op*) list_get(query_conectada, 0);

        switch(op){
            case EJECUTAR_QUERY: {
                int id = *(int*) list_get(query_conectada, 1);
                char* archivo_tmp = (char*) list_get(query_conectada, 2);
                char* archivo = strdup(archivo_tmp);
                char* prioridad_str = (char*) list_get(query_conectada, 3);
                int prioridad = atoi(prioridad_str);

                query* nueva_query = crear_query(conexion, id, archivo, prioridad);
                pthread_mutex_lock(&mutex_workers);
                log_info(logger, "## Se conecta un Query Control para ejecutar la Query <%s> con prioridad <%d> - Id asignado: <%d>. Nivel multiprocesamiento <%d>", archivo, prioridad, nueva_query->id, list_size(workers));
                pthread_mutex_unlock(&mutex_workers);

                free(archivo);

                pthread_mutex_lock(&mutex_querys);
                list_add(querys, nueva_query);
                if(strcmp(planificador, "PRIORIDADES") == 0){
                    list_sort(querys, comparar_queries);
                }
                pthread_mutex_unlock(&mutex_querys);

                if(strcmp(planificador,"PRIORIDADES") == 0){
                    pthread_mutex_lock(&mutex_querys);
                    query* query_tope = list_get(querys, 0);
                    
                    pthread_mutex_lock(&mutex_workers_libres);
                    if(list_size(workers_libres) == 0 && query_tope->id == nueva_query->id){
                        pthread_mutex_unlock(&mutex_workers_libres);

                        pthread_mutex_lock(&mutex_workers_ocupados);
                        pthread_mutex_lock(&mutex_querys_activas);
                        int id_query_a_desalojar = query_a_desalojar(nueva_query->prioridad);
                        
                        if(id_query_a_desalojar >= 0){
                            query* query_desalojada = buscar_query_por_id(id_query_a_desalojar);
                            worker* worker_a_liberar = buscar_worker_con_query(query_desalojada->id);

                            if(worker_a_liberar != NULL){
                            list_remove_element(querys, nueva_query);
                            list_add(querys_activas, nueva_query);
                            nueva_query->aging_activo = false;
                            query_desalojada->en_desalojo = true;
                            worker_a_liberar->id_query = nueva_query;

                            int id_nueva_query = nueva_query->id;
                            int pc_nueva_query = nueva_query->pc;
                            pthread_mutex_unlock(&mutex_querys_activas);
                            pthread_mutex_unlock(&mutex_workers_ocupados);
                            pthread_mutex_unlock(&mutex_querys);

                            t_worker_op op = DESALOJO_QUERY;
                            t_paquete* solicitud_a_worker = crear_paquete();
                            agregar_a_paquete(solicitud_a_worker, &op, sizeof(t_worker_op));
                            agregar_a_paquete(solicitud_a_worker, &id_nueva_query, sizeof(int));
                            char* nombre_nueva_query = strdup(nueva_query->archivo);
                            agregar_a_paquete(solicitud_a_worker, nombre_nueva_query, strlen(nombre_nueva_query)+1);
                            agregar_a_paquete(solicitud_a_worker, &pc_nueva_query, sizeof(int));
                            enviar_paquete(solicitud_a_worker, worker_a_liberar->fd);
                            eliminar_paquete(solicitud_a_worker);
                            free(nombre_nueva_query);
                            continue;
                        } else {
                            pthread_mutex_unlock(&mutex_querys_activas);
                            pthread_mutex_unlock(&mutex_workers_ocupados);
                            pthread_mutex_unlock(&mutex_querys);
                            log_warning(logger, "Intento de desalojo fallido: La Query %d ya no estaba en el worker.", id_query_a_desalojar);
                            }
                        }
                        pthread_mutex_unlock(&mutex_querys_activas);
                        pthread_mutex_unlock(&mutex_workers_ocupados);
                    } else {
                        pthread_mutex_unlock(&mutex_workers_libres);
                    }

                    nueva_query->aging_activo = true;
                    nueva_query->tiempo_llegada = obtener_tiempo_llegada();
                    pthread_mutex_unlock(&mutex_querys);

                    pthread_create(&nueva_query->hilo_aging, NULL, aging_individual, nueva_query);
                    pthread_detach(nueva_query->hilo_aging);
                }

                sem_wait(&sem_workers_libres);

                pthread_mutex_lock(&mutex_querys);
                pthread_mutex_lock(&mutex_workers_libres);
                pthread_mutex_lock(&mutex_workers_ocupados);
                pthread_mutex_lock(&mutex_querys_activas);

                if(list_size(querys) > 0 && list_size(workers_libres) > 0){
                    worker* worker_destino = (worker*) list_remove(workers_libres, 0);
                    list_add(workers_ocupados, worker_destino);

                    query* query_a_ejecutar;
                    if(strcmp(planificador,"FIFO") == 0){
                        query_a_ejecutar = (query*) list_remove(querys, 0);
                    } else {
                        list_sort(querys, comparar_queries);
                        query_a_ejecutar = (query*) list_remove(querys, 0);
                        query_a_ejecutar->aging_activo = false;
                    }

                    list_add(querys_activas, query_a_ejecutar);
                    worker_destino->id_query = query_a_ejecutar;


                    log_info(logger, "## Se envía la Query %d (%d) al Worker %d",query_a_ejecutar->id,query_a_ejecutar->prioridad,worker_destino->id);
                    int enviar_id = query_a_ejecutar->id;
                    int enviar_pc = query_a_ejecutar->pc;
                    pthread_mutex_unlock(&mutex_querys_activas);
                    pthread_mutex_unlock(&mutex_workers_ocupados);
                    pthread_mutex_unlock(&mutex_workers_libres);
                    pthread_mutex_unlock(&mutex_querys);

                    //log_info(logger, "El worker asignado es: %d", worker_destino->id);

                    char* nombre_query_run = strdup(query_a_ejecutar->archivo);
                    t_worker_op op = WORKER_LIBRE;
                    t_paquete* solicitud_a_worker = crear_paquete();
                    agregar_a_paquete(solicitud_a_worker, &op, sizeof(t_worker_op));
                    agregar_a_paquete(solicitud_a_worker, &enviar_id, sizeof(int));
                    agregar_a_paquete(solicitud_a_worker, nombre_query_run, strlen(nombre_query_run)+1);
                    agregar_a_paquete(solicitud_a_worker, &enviar_pc, sizeof(int));
                    enviar_paquete(solicitud_a_worker, worker_destino->fd);
                    eliminar_paquete(solicitud_a_worker);
                    free(nombre_query_run);
                } else {
                    pthread_mutex_unlock(&mutex_querys_activas);
                    pthread_mutex_unlock(&mutex_workers_ocupados);
                    pthread_mutex_unlock(&mutex_workers_libres);
                    pthread_mutex_unlock(&mutex_querys);
                    sem_post(&sem_workers_libres);
                }
                break;
        }
        case DESCONEXION_QUERY: {
            int id_query_desconectada = *(int*) list_get(query_conectada, 1);
            query* query_desconectada;
            pthread_mutex_lock(&mutex_querys);
            query_desconectada = buscar_query_ready_por_id(id_query_desconectada);
                
                //LA busca en Ready
            if (query_desconectada != NULL){
                log_info(logger, "La query ID: %d Se desconectó abruptamente. Estaba en Ready", id_query_desconectada);
                pthread_mutex_unlock(&mutex_querys);

                query_desconectada->aging_activo = false;
                list_remove_element(querys,query_desconectada);
                pthread_mutex_unlock(&mutex_querys);

                pthread_mutex_lock(&mutex_querys_fin);
                list_add(querys_fin, query_desconectada);
                pthread_mutex_unlock(&mutex_querys_fin);

                //list_destroy_and_destroy_elements(query_conectada, free);
                close(conexion);
                return NULL;

            } else {//Si no está en ready, está en RUN.
                
                pthread_mutex_lock(&mutex_workers_ocupados);
                pthread_mutex_lock(&mutex_querys_activas);
                query_desconectada = buscar_query_por_id(id_query_desconectada);;
                query_desconectada->en_desalojo = true; 
                worker* worker_a_desalojar = buscar_worker_con_query(id_query_desconectada);
                if(worker_a_desalojar!=NULL){
                    log_info(logger, "La query ID: %d Se desconectó abruptamente. Estaba en el Worker: %d", id_query_desconectada, worker_a_desalojar->id);
                    list_remove_element(querys_activas, query_desconectada);
                } else {
                    pthread_mutex_unlock(&mutex_querys_activas);
                    pthread_mutex_unlock(&mutex_workers_ocupados);
                    pthread_mutex_unlock(&mutex_querys);
                    break;
                }
                pthread_mutex_unlock(&mutex_querys_activas);
                pthread_mutex_unlock(&mutex_workers_ocupados);
                pthread_mutex_unlock(&mutex_querys);
 
 
                pthread_mutex_lock(&mutex_querys_fin);
                list_add(querys_fin, query_desconectada);
                pthread_mutex_unlock(&mutex_querys_fin);

                t_worker_op op = DESALOJO_PASIVO;
                t_paquete* solicitud_desalojo_pasivo = crear_paquete();
                agregar_a_paquete(solicitud_desalojo_pasivo, &op, sizeof(t_worker_op));
                enviar_paquete(solicitud_desalojo_pasivo, worker_a_desalojar->fd);
                eliminar_paquete(solicitud_desalojo_pasivo);

                //list_destroy_and_destroy_elements(query_conectada, free);
                close(conexion);
                return NULL;
            }
            break;
        }
        default:
            log_error(logger, "La query envió un paquete incompleto con %d argumentos",list_size(query_conectada));
            if(query_conectada) list_destroy_and_destroy_elements(query_conectada, free);
            close(conexion);
        }
        list_destroy_and_destroy_elements(query_conectada, free);
    }
    //while(1){}  ver aca como cerrar la conexion

    return NULL;
}

void manejar_worker(void* arg) {
    worker* worker_manejado = (worker*) arg;
    int fd_worker = worker_manejado->fd;

    while (1) {
        t_list* respuesta_de_worker = recibir_paquete(fd_worker);
        if (!respuesta_de_worker) {
            pthread_mutex_lock(&mutex_workers);
            int cantidad_total_workers = list_size(workers);
            pthread_mutex_unlock(&mutex_workers);
            int id_query = (worker_manejado->id_query != NULL) ? worker_manejado->id_query->id : -1;
            log_warning(logger, "## Se desconecta el Worker %d - Se finaliza la Query %d - Cantidad total de Workers: %d", worker_manejado->id, id_query, cantidad_total_workers - 1);
            
            bool worker_ocupado = es_worker_ocupado(worker_manejado->id);
            if(worker_ocupado){
                pthread_mutex_lock(&mutex_workers_ocupados);
                list_remove_element(workers_ocupados, worker_manejado);
                pthread_mutex_unlock(&mutex_workers_ocupados);
            } else {
                pthread_mutex_lock(&mutex_workers_libres);
                list_remove_element(workers_libres, worker_manejado);
                pthread_mutex_unlock(&mutex_workers_libres);
            }
            
            close(fd_worker);
            break;
        }

        t_master_op op = *(t_master_op*) list_get(respuesta_de_worker, 0);

        switch (op) {
            case LECTURA: {
                int id_query  = *(int*) list_get(respuesta_de_worker, 1);   
                char* valor = strdup(list_get(respuesta_de_worker, 2));
                char* file = strdup(list_get(respuesta_de_worker, 3));
                char* tag = strdup(list_get(respuesta_de_worker, 4));
                pthread_mutex_lock(&mutex_querys_activas);
                query* query_lectura = buscar_query_por_id(id_query);
                if(query_lectura != NULL){
                pthread_mutex_unlock(&mutex_querys_activas);
                log_info(logger, "## Se envía un mensaje de lectura de la Query %d en el Worker %d al Query Control", id_query, worker_manejado->id);
                t_paquete* devolucion_query = crear_paquete();
                agregar_a_paquete(devolucion_query, &op, sizeof(t_master_op));
                agregar_a_paquete(devolucion_query, &id_query, sizeof(int));
                agregar_a_paquete(devolucion_query, valor, strlen(valor)+1);
                agregar_a_paquete(devolucion_query, file, strlen(file)+1);
                agregar_a_paquete(devolucion_query, tag, strlen(tag)+1);
                enviar_paquete(devolucion_query, query_lectura->fd_query);
                free(valor);
                free(file);
                free(tag);
                eliminar_paquete(devolucion_query);
                break;
                } else {
                    pthread_mutex_unlock(&mutex_querys_activas);
                    free(valor);
                    free(file);
                    free(tag);
                    log_warning(logger,"##Interrupción en la Lectura");
                    break;
                }
            }
            case FIN: {
                int id_query = *(int*) list_get(respuesta_de_worker, 1);
                char* archivo_str = strdup(list_get(respuesta_de_worker, 2));
                char* motivo = "Fin";
                

                pthread_mutex_lock(&mutex_querys);
                pthread_mutex_lock(&mutex_workers_ocupados);
                pthread_mutex_lock(&mutex_querys_activas);
                log_info(logger,"## Se terminó la Query %d en el Worker %d", id_query, worker_manejado->id);
                query* query_fin = buscar_query_por_id(id_query);

                if (!query_fin) {
                    log_error(logger,"No se encontró la Query %d en la lista de queries activas",id_query);
                    free(archivo_str);
                pthread_mutex_unlock(&mutex_workers_ocupados);
                pthread_mutex_unlock(&mutex_querys_activas);
                pthread_mutex_unlock(&mutex_querys);
                    break;
                } else {
                    pthread_mutex_unlock(&mutex_workers_ocupados);
                }

                list_remove_element(querys_activas, query_fin);
                pthread_mutex_unlock(&mutex_querys_activas);

                pthread_mutex_lock(&mutex_querys_fin);
                list_add(querys_fin, query_fin);
                pthread_mutex_unlock(&mutex_querys_fin);

                t_paquete* devolucion_query = crear_paquete();
                agregar_a_paquete(devolucion_query, &op, sizeof(t_master_op));
                agregar_a_paquete(devolucion_query, &id_query, sizeof(int));
                agregar_a_paquete(devolucion_query, archivo_str, strlen(archivo_str) + 1);
                agregar_a_paquete(devolucion_query, motivo, strlen(motivo) + 1);
                enviar_paquete(devolucion_query, query_fin->fd_query);
                eliminar_paquete(devolucion_query);
                free(archivo_str);



                if (list_size(querys)){
                    int id_query_lista;
                    int pc_query_lista;
                    char* archivo_query_lista;
                    int prioridad_query_lista;
                    query* query_lista;

                    if(strcmp(planificador,"FIFO") == 0){
                    query_lista = (query*) list_remove(querys, 0);
                    }

                    if(strcmp(planificador,"PRIORIDADES") == 0){
                    list_sort(querys, comparar_queries);
                    query_lista = (query*) list_remove(querys, 0);
                    query_lista->aging_activo = false;
                    }
                    archivo_query_lista = strdup(query_lista->archivo);
                    id_query_lista = query_lista->id;
                    pc_query_lista = query_lista->pc;
                    prioridad_query_lista = query_lista->prioridad;

                    pthread_mutex_lock(&mutex_workers_ocupados);
                    pthread_mutex_lock(&mutex_querys_activas);
                    list_add(querys_activas, query_lista);
                    worker_manejado->id_query = query_lista;
                    log_info(logger, "## Se envía la Query %d - %s (%d) al Worker %d", id_query_lista, archivo_query_lista, prioridad_query_lista, worker_manejado->id);

                    pthread_mutex_unlock(&mutex_querys_activas);
                    pthread_mutex_unlock(&mutex_workers_ocupados);
                    pthread_mutex_unlock(&mutex_querys);


                    t_paquete* solicitud_a_worker = crear_paquete();
                    t_worker_op op_worker = WORKER_LIBRE;
                    agregar_a_paquete(solicitud_a_worker, &op_worker, sizeof(t_worker_op));
                    agregar_a_paquete(solicitud_a_worker, &id_query_lista, sizeof(int));
                    agregar_a_paquete(solicitud_a_worker, archivo_query_lista, strlen(archivo_query_lista) + 1);
                    agregar_a_paquete(solicitud_a_worker, &pc_query_lista, sizeof(int));
                    enviar_paquete(solicitud_a_worker, fd_worker);
                    eliminar_paquete(solicitud_a_worker);
                    free(archivo_query_lista);

                    
                } else {
                    pthread_mutex_unlock(&mutex_querys);

                    pthread_mutex_lock(&mutex_workers_libres);
                    pthread_mutex_lock(&mutex_workers_ocupados);
                    list_remove_element(workers_ocupados, worker_manejado);
                    list_add(workers_libres, worker_manejado);
                    pthread_mutex_unlock(&mutex_workers_ocupados);
                    pthread_mutex_unlock(&mutex_workers_libres);
                    log_info(logger,"Worker %d ahora está libre",worker_manejado->id);
                    sem_post(&sem_workers_libres);
                }
                break;
            }
            case DESALOJO: {
                int id_query_desalojada = *(int*) list_get(respuesta_de_worker, 1);

                //La busca en Queries Activas si fue desalojo 
                pthread_mutex_lock(&mutex_querys_activas);
                query* query_desalojada = buscar_query_por_id(id_query_desalojada);

                if (query_desalojada != NULL) {
                    list_remove_element(querys_activas, query_desalojada);
                    pthread_mutex_unlock(&mutex_querys_activas);
                    char* archivo_query_desalojada = list_get(respuesta_de_worker, 2);
                    int pc = *(int*) list_get(respuesta_de_worker, 3);
                    char* archivo_paquete = strdup(archivo_query_desalojada);
                    char* motivo = strdup("Prioridad");
                    
                    pthread_mutex_lock(&mutex_querys);
                    query_desalojada->pc=pc;

                    bool ya_esta_en_ready = false;
                    for (int i = 0; i < list_size(querys); i++) {
                        query* q = list_get(querys, i);
                        if (q->id == query_desalojada->id) {
                            q->en_desalojo = false;
                            ya_esta_en_ready = true;
                            pthread_mutex_unlock(&mutex_querys);
                            break;
                        }
                    }
                    if (!ya_esta_en_ready){
                        query_desalojada->tiempo_llegada = obtener_tiempo_llegada();
                        query_desalojada->aging_activo = true;
                        list_add(querys, query_desalojada);
                        query_desalojada->en_desalojo = false;
                        pthread_mutex_unlock(&mutex_querys);

                        pthread_create(&query_desalojada->hilo_aging, NULL, aging_individual, query_desalojada);
                        pthread_detach(query_desalojada->hilo_aging);

                    } else {
                        pthread_mutex_unlock(&mutex_querys);
                    }
                
                    t_paquete* devolucion_query = crear_paquete();
                    agregar_a_paquete(devolucion_query, &op, sizeof(t_master_op));
                    agregar_a_paquete(devolucion_query, &id_query_desalojada, sizeof(int));
                    agregar_a_paquete(devolucion_query, archivo_paquete, strlen(archivo_paquete) + 1);
                    agregar_a_paquete(devolucion_query, &pc, sizeof(int));
                    agregar_a_paquete(devolucion_query, motivo, strlen(motivo) + 1);
                    //log_info(logger,"Se notifica a la Query desalojada: ID: %d. Archivo: %s. PC: %d. Prioridad: %d",query_desalojada->id,query_desalojada->archivo, query_desalojada->pc, query_desalojada->prioridad);
                    log_info(logger,"## Se desaloja la Query %d (%d) del Worker %d - Motivo: %s",query_desalojada->id,query_desalojada->prioridad,worker_manejado->id,motivo);
                    enviar_paquete(devolucion_query, query_desalojada->fd_query);
                    eliminar_paquete(devolucion_query);
                    free(motivo);
                    free(archivo_paquete);

                    query* query_run = worker_manejado->id_query;

                    if (query_run != NULL && query_run->id != id_query_desalojada) {
                        char* archivo_nuevo = strdup(query_run->archivo);
                        log_info(logger, "## Se envía la Query %d (%d) al Worker %d", query_run->id, query_run->prioridad, worker_manejado->id);
                        t_paquete* paquete_otra_query = crear_paquete();
                        t_worker_op op_worker = WORKER_LIBRE;
                        agregar_a_paquete(paquete_otra_query, &op_worker, sizeof(t_worker_op));
                        agregar_a_paquete(paquete_otra_query, &query_run->id, sizeof(int));
                        agregar_a_paquete(paquete_otra_query, archivo_nuevo, strlen(archivo_nuevo) + 1);
                        agregar_a_paquete(paquete_otra_query, &query_run->pc, sizeof(int));
                        enviar_paquete(paquete_otra_query, fd_worker);
                        eliminar_paquete(paquete_otra_query);
                        free(archivo_nuevo);
                    } else {
                        pthread_mutex_lock(&mutex_querys);
                        if (list_is_empty(querys)) {
                            pthread_mutex_unlock(&mutex_querys);
                            log_info(logger, "No hay Queries listas para ejecutar tras el desalojo");
                            break;
                        }
                        if(list_size(querys)>1) list_sort(querys, comparar_queries);
                        query_run = (query*) list_remove(querys, 0);
                        query_run->aging_activo = false;
                        pthread_mutex_unlock(&mutex_querys);

                        if (!query_run) {
                            log_error(logger, "Error: no se encontro una Query lista para ejecutar tras el desalojo");
                            break;
                        }
                        worker_manejado->id_query = query_run;
                        char* archivo_nuevo = strdup(query_run->archivo);
                        log_info(logger,"Asignando nueva Query al Worker %d -> ID=%d, Archivo=%s, PC=%d",worker_manejado->id,query_run->id,archivo_nuevo,query_run->pc);
                        log_info(logger, "## Se envía la Query %d (%d) al Worker %d", query_run->id, query_run->prioridad, worker_manejado->id);
                        t_paquete* paquete_otra_query = crear_paquete();
                        t_worker_op op_worker = WORKER_LIBRE;
                        agregar_a_paquete(paquete_otra_query, &op_worker, sizeof(t_worker_op));
                        agregar_a_paquete(paquete_otra_query, &query_run->id, sizeof(int));
                        agregar_a_paquete(paquete_otra_query, archivo_nuevo, strlen(archivo_nuevo) + 1);
                        agregar_a_paquete(paquete_otra_query, &query_run->pc, sizeof(int));
                        enviar_paquete(paquete_otra_query, fd_worker);
                        eliminar_paquete(paquete_otra_query);
                        free(archivo_nuevo);

                        pthread_mutex_lock(&mutex_querys_activas);
                        list_add(querys_activas, query_run);
                        pthread_mutex_unlock(&mutex_querys_activas);
                    }
                    break;
                } else {
                    pthread_mutex_unlock(&mutex_querys_activas);

                    pthread_mutex_lock(&mutex_querys_fin);
                    pthread_mutex_lock(&mutex_querys);
                    query* query_desconectada = buscar_query_fin_por_id(id_query_desalojada);

                    if(list_size(querys)>0 && query_desconectada != NULL) {
                        list_sort(querys, comparar_queries);
                        query* query_run = (query*) list_remove(querys, 0);
                        query_run->aging_activo = false;

                        pthread_mutex_lock(&mutex_workers_ocupados);
                        worker_manejado->id_query = query_run;

                        pthread_mutex_lock(&mutex_querys_activas);
                        list_add(querys_activas, query_run);
                        
                        pthread_mutex_unlock(&mutex_querys_activas);
                        pthread_mutex_unlock(&mutex_workers_ocupados);                    
                        pthread_mutex_unlock(&mutex_querys);
                        pthread_mutex_unlock(&mutex_querys_fin);

                        char* archivo_nuevo = strdup(query_run->archivo);
                        log_info(logger,"Asignando nueva Query al Worker %d -> ID=%d, Archivo=%s, PC=%d",worker_manejado->id,query_run->id,archivo_nuevo,query_run->pc);
                        log_info(logger, "## Se envía la Query %d (%d) al Worker %d", query_run->id, query_run->prioridad, worker_manejado->id);
                        t_paquete* paquete_otra_query = crear_paquete();
                        t_worker_op op_worker = WORKER_LIBRE;
                        agregar_a_paquete(paquete_otra_query, &op_worker, sizeof(t_worker_op));
                        agregar_a_paquete(paquete_otra_query, &query_run->id, sizeof(int));
                        agregar_a_paquete(paquete_otra_query, archivo_nuevo, strlen(archivo_nuevo) + 1);
                        agregar_a_paquete(paquete_otra_query, &query_run->pc, sizeof(int));
                        enviar_paquete(paquete_otra_query, fd_worker);
                        eliminar_paquete(paquete_otra_query);
                        free(archivo_nuevo);
                        break;

                    } else if(query_desconectada!=NULL){
                        pthread_mutex_unlock(&mutex_querys);
                        pthread_mutex_unlock(&mutex_querys_fin);

                        pthread_mutex_lock(&mutex_workers_ocupados);
                        list_remove_element(workers_ocupados, worker_manejado);
                        pthread_mutex_unlock(&mutex_workers_ocupados);

                        pthread_mutex_lock(&mutex_workers_libres);
                        list_add(workers_libres, worker_manejado);
                        log_info(logger,"Worker %d ahora está libre",worker_manejado->id);
                        pthread_mutex_unlock(&mutex_workers_libres);

                        sem_post(&sem_workers_libres);
                        break;
                    }
                }
            }
            case DESCONEXION_WORKER: {
                int id_query_cancelada = *(int*) list_get(respuesta_de_worker, 1);
                char* archivo_query_cancelada = list_get(respuesta_de_worker, 2);
                int pc = *(int*) list_get(respuesta_de_worker, 3);
                char* archivo_paquete = strdup(archivo_query_cancelada);                
                
                if(id_query_cancelada != -1){

                    char* motivo = strdup("Desconexión abrupta de Worker");
//                    log_info(logger,"Worker %d interrumpió la Query %d - Archivo: %s - PC: %d",worker_manejado->id,id_query_cancelada,archivo_query_cancelada,pc);
                    
                    pthread_mutex_lock(&mutex_querys);
                    pthread_mutex_lock(&mutex_workers_ocupados);
                    worker_manejado->id_query = NULL;
                    query* query_cancelada = buscar_query_ready_por_id(id_query_cancelada);

                    pthread_mutex_lock(&mutex_querys_activas);
                    if (query_cancelada == NULL) query_cancelada = buscar_query_por_id(id_query_cancelada);
                    if (query_cancelada != NULL) {query_cancelada->pc = pc;}
                    pthread_mutex_unlock(&mutex_querys_activas);

                    list_remove_element(workers_ocupados,worker_manejado);
                    pthread_mutex_unlock(&mutex_workers_ocupados);

                    pthread_mutex_unlock(&mutex_querys);

                    if (query_cancelada != NULL) {
//                        log_info(logger,"## Se desaloja la Query %d (%d) del Worker %d - Motivo: %s",id_query_cancelada, query_cancelada->prioridad, worker_manejado->id, motivo);
                        t_paquete* devolucion_query = crear_paquete();
                        agregar_a_paquete(devolucion_query, &op, sizeof(t_master_op));
                        agregar_a_paquete(devolucion_query, &id_query_cancelada, sizeof(int));
                        agregar_a_paquete(devolucion_query, archivo_paquete, strlen(archivo_paquete) + 1);
                        agregar_a_paquete(devolucion_query, motivo, strlen(motivo) + 1);
                        enviar_paquete(devolucion_query, query_cancelada->fd_query);
                        eliminar_paquete(devolucion_query);
                    }

                    pthread_mutex_lock(&mutex_workers);
                    list_remove_element(workers, worker_manejado);
                    pthread_mutex_unlock(&mutex_workers);

                    free(archivo_paquete);
                    free(motivo);
                    
                } else {
                    pthread_mutex_lock(&mutex_workers_libres);
                    list_remove_element(workers_libres,worker_manejado);
                    pthread_mutex_unlock(&mutex_workers_libres);
                }
                pthread_mutex_lock(&mutex_workers);
                list_remove_element(workers,worker_manejado);
                log_info(logger,"Se desconecta el Worker <%d> - Se finaliza la Query <%d> - Cantidad de workers totales <%d>",worker_manejado->id,id_query_cancelada,list_size(workers));
                pthread_mutex_unlock(&mutex_workers);                
                pthread_exit(NULL);
                break;
            }
            default:
                log_warning(logger, "Respuesta desconocida de Worker %d", worker_manejado->id);
            break;
        }

        list_destroy_and_destroy_elements(respuesta_de_worker, free);
    }
}

int recibir_handshake(int conexion)
{
    int recepcion;
    recv(conexion,&recepcion,sizeof(int),MSG_WAITALL);
    if(recepcion == 20){
        int ok = 1;
        send(conexion,&ok,sizeof(int),0);
        return recepcion;
    } else if(recepcion < 20 && recepcion >0){
        int ok = 1;
        send(conexion,&ok,sizeof(int),0);
        return recepcion;
    }    
    return 0;
}

worker* crear_worker(int fd, int id, query* query){
    worker* nuevo_worker = malloc(sizeof(worker));
    nuevo_worker->fd = fd;
    nuevo_worker->id = id;
    nuevo_worker->id_query = query;
    return nuevo_worker;
}

query* crear_query(int conexion,int id, char* archivo, int prioridad){
    query* nueva_query = malloc(sizeof(query));
    nueva_query->id = id;
    nueva_query->pc = 0;
    nueva_query->archivo = strdup(archivo);
    nueva_query->prioridad = prioridad;
    nueva_query->fd_query = conexion;
    nueva_query->tiempo_llegada = obtener_tiempo_llegada();
    nueva_query->aging_activo = false;
    pthread_t hilo;
    nueva_query->hilo_aging = hilo;
    nueva_query->en_desalojo = false;
    return nueva_query;
}

void destruir_worker(void* worker_a_destruir){
    worker* w = (worker*) worker_a_destruir;
    free(w);
}

void destruir_query(void* query_a_destruir){
    query* q = (query*) query_a_destruir;
    free(q->archivo);
    free(q);
}

void terminar_master(int signo){
    log_warning(logger, "Finalizando Modulo Master...");
    pthread_mutex_lock(&mutex_workers);
    list_destroy_and_destroy_elements(workers, destruir_worker);
    pthread_mutex_unlock(&mutex_workers);
    pthread_mutex_lock(&mutex_querys);
    list_destroy_and_destroy_elements(querys, destruir_query);
    pthread_mutex_unlock(&mutex_querys);
    close(master_fd);
    log_destroy(logger);
    config_destroy(config);
    exit(0);
}

bool comparar_queries(void* q_a, void* q_b){
    query* q1 = (query*) q_a;
    query* q2 = (query*) q_b;

    if(q1->prioridad == q2->prioridad){
        return q1->tiempo_llegada < q2->tiempo_llegada;
    }
    return q1->prioridad < q2->prioridad;    
}

query* buscar_query_por_id(int id_buscar) {
    //pthread_mutex_lock(&mutex_querys_activas);
    int size = list_size(querys_activas);
    for (int i = 0; i < size; i++) {
        query* q = list_get(querys_activas, i);
        if (q->id == id_buscar) {
            //pthread_mutex_unlock(&mutex_querys_activas);
            return q;
        }
    }
    //pthread_mutex_unlock(&mutex_querys_activas);
    return NULL;
}

query* buscar_query_ready_por_id(int id_buscar) {
    //pthread_mutex_lock(&mutex_querys);
    int size = list_size(querys);
    for (int i = 0; i < size; i++) {

        query* q = (query*) list_get(querys, i);

        if (q->id == id_buscar) {
    //pthread_mutex_unlock(&mutex_querys);
            return q;
        }
    }
    //pthread_mutex_unlock(&mutex_querys);
    return NULL;

}

query* buscar_query_fin_por_id(int id_buscar) {
    //pthread_mutex_lock(&mutex_querys_fin);
    int size = list_size(querys_fin);
    for (int i = 0; i < size; i++) {
        query* q = (query*) list_get(querys_fin, i);
        if (q->id == id_buscar) {
    //pthread_mutex_unlock(&mutex_querys_fin);
            return q;
        }
    }
    //pthread_mutex_unlock(&mutex_querys_fin);
    return NULL;

}


int query_a_desalojar(int prioridad){
    int id = -1;
    int prioridad_auxiliar = prioridad;

    //pthread_mutex_lock(&mutex_querys_activas);
    int size = list_size(querys_activas);
    if (size < 1){
    //pthread_mutex_unlock(&mutex_querys_activas);
        return -1;
    }

    for (int i = 0; i < size; i++) {
        query* q = list_get(querys_activas, i);
        if (q->prioridad > prioridad_auxiliar && !q->en_desalojo) {
            prioridad_auxiliar = q->prioridad;
            id = q->id;
        }
    }
    //pthread_mutex_unlock(&mutex_querys_activas);

    return id;
}


worker* buscar_worker_con_query(int id_query){
    //pthread_mutex_lock(&mutex_workers_ocupados);
    for(int i = 0;i < list_size(workers_ocupados);i++){
        worker* w = (worker*) list_get(workers_ocupados, i);
        if(w->id_query->id == id_query){
    //pthread_mutex_unlock(&mutex_workers_ocupados);
            return w;
        }
    }
    //pthread_mutex_unlock(&mutex_workers_ocupados);
    return NULL;

}

bool es_worker_ocupado(int id_buscar) {
    int size = list_size(workers_ocupados);
    for (int i = 0; i < size; i++) {
        worker* w = list_get(workers_ocupados, i);
        if (w->id == id_buscar) {
            return true;
        }
    }
    return false;
}



bool es_worker_libre(int id_buscar) {
    int size = list_size(workers_libres);
    for (int i = 0; i < size; i++) {
        worker* w = list_get(workers_libres, i);
        if (w->id == id_buscar) {
            return true;
        }
    }
    return false;
}

void* aging_individual(void* arg) {
    query* query_aging = (query*)arg;
    while(1){
        usleep(tiempo_aging * 1000);  // dormir X ms

        pthread_mutex_lock(&mutex_querys);
        if(!query_aging->aging_activo || query_aging == NULL){
            pthread_mutex_unlock(&mutex_querys);
            return NULL;         
        }

        if(query_aging->prioridad > 0 ){
            int anterior = query_aging->prioridad;
            query_aging->prioridad--;
            query_aging->tiempo_llegada = obtener_tiempo_llegada();
            log_info(logger, "##Query <%d> Cambio de prioridad: <%d> - <%d>", query_aging->id, anterior, query_aging->prioridad);
            list_sort(querys, comparar_queries);
        }else {
            pthread_mutex_unlock(&mutex_querys);
            pthread_exit(NULL);
            return NULL;
        }

        pthread_mutex_lock(&mutex_workers_ocupados);
        pthread_mutex_lock(&mutex_querys_activas);
        query* query_a_ejecutar = revisar_desalojo_por_aging();

        if(query_a_ejecutar != NULL){
            int id_query_a_desalojar = query_a_desalojar(query_a_ejecutar->prioridad);
            query* query_desalojada_por_aging = buscar_query_por_id(id_query_a_desalojar);
            worker* worker_a_liberar = buscar_worker_con_query(query_desalojada_por_aging->id);

            if(worker_a_liberar != NULL){
                query_desalojada_por_aging->en_desalojo = true;
                worker_a_liberar->id_query = query_a_ejecutar;

                list_remove_element(querys, query_a_ejecutar);
                list_add(querys_activas, query_a_ejecutar);

                query_a_ejecutar->aging_activo = false;
                int id = query_a_ejecutar->id;
                int pc = query_a_ejecutar->pc;
                int fd_worker = worker_a_liberar->fd;
                char* nombre_query_aging = strdup(query_a_ejecutar->archivo);
                pthread_mutex_unlock(&mutex_querys_activas);
                pthread_mutex_unlock(&mutex_workers_ocupados);
                pthread_mutex_unlock(&mutex_querys);

                    //Envia la query al worker.
                t_worker_op op = DESALOJO_QUERY;
                t_paquete* solicitud_a_worker = crear_paquete();
                agregar_a_paquete(solicitud_a_worker, &op, sizeof(t_worker_op));
                agregar_a_paquete(solicitud_a_worker, &id, sizeof(int));
                agregar_a_paquete(solicitud_a_worker, nombre_query_aging, strlen(nombre_query_aging)+1);
                agregar_a_paquete(solicitud_a_worker, &pc, sizeof(int));
                enviar_paquete(solicitud_a_worker, worker_a_liberar->fd);
                eliminar_paquete(solicitud_a_worker);
                free(nombre_query_aging);

                return NULL;
            }else{
                log_error(logger, "Error crítico: Query <%d> desalojada por Aging no se encontró en el worker.", query_desalojada_por_aging->id);            
                pthread_mutex_unlock(&mutex_querys_activas);
                pthread_mutex_unlock(&mutex_workers_ocupados);
                pthread_mutex_unlock(&mutex_querys);
            }
        } else {

            if(query_aging->prioridad==0){
                query_aging->aging_activo = false;
            pthread_mutex_unlock(&mutex_querys_activas);
            pthread_mutex_unlock(&mutex_workers_ocupados);
            pthread_mutex_unlock(&mutex_querys);
            return NULL;
            }

            pthread_mutex_unlock(&mutex_querys_activas);
            pthread_mutex_unlock(&mutex_workers_ocupados);
            pthread_mutex_unlock(&mutex_querys);
        }

            pthread_mutex_lock(&mutex_querys_fin);
            if(buscar_query_fin_por_id(query_aging->id)){
                pthread_mutex_unlock(&mutex_querys_fin);
                return NULL;
            } else {
                pthread_mutex_unlock(&mutex_querys_fin);
            }
            
    }
    return NULL;
}


query* revisar_desalojo_por_aging(){
    //Se llama dentro de una zona en mutex.
    if(list_size(querys)<1){
        return NULL;
    }
        query* primera_query = (query*) list_get(querys, 0);
        //También necesita mutex_querys_activas
        int id_query_a_desalojar = query_a_desalojar(primera_query->prioridad);
        if(id_query_a_desalojar >= 0){
            log_info(logger, "Query con prioridad por Aging Id:%d. Archivo: %s. Prioridad: %d. PC: %d.", primera_query->id, primera_query->archivo, primera_query->prioridad, primera_query->pc);

        return primera_query;
        }

    return NULL;
}
