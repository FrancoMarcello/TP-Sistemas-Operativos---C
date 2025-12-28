#include <worker.h>

int conexion_storage;
int conexion_master;
void* espacio_de_usuario;
t_list* marcos;
t_list* marcos_libres;
t_list* marcos_ocupados;
t_list* tablas_paginas;
t_list* paginas_presentes;
int tam_memoria;
int tam_bloque;
int retardo_memoria;
char* algoritmo_reemplazo;
pthread_mutex_t mutex_paginas_presentes;
pthread_mutex_t mutex_tablas;
pthread_mutex_t mutex_marcos;
pthread_mutex_t mutex_espacio_usuario;

pthread_mutex_t mutex_query_ejecutando = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_query_prioridad = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_socket_storage = PTHREAD_MUTEX_INITIALIZER;

query_worker_h* query_en_worker = NULL;  // la query que se está ejecutando
query_worker_h* query_prioridad = NULL;  // la query que llegó con mayor prioridad

int main(int argc, char* argv[]) {

    signal(SIGINT, terminar_worker_forzado); //Ctrl+C
    signal(SIGHUP, terminar_worker_forzado); //Cierre de terminal.

    // puesto de ejemplo para probar la comunicacion
    config = config_create("worker.config");
    logger = log_create("worker.log","Worker",1,LOG_LEVEL_INFO);

    if(argc < 3){
        printf("Faltan argumentos");
        exit(1);
    }

    char* path_config = argv[1];
    int id_worker = atoi(argv[2]);
    char* id_worker_storage = argv[2];

    char* ip_master;
    char* puerto_master;
    char* ip_storage;
    char* puerto_storage;
    char* path_scripts;
    char* log_level;

    t_config* config = config_create(path_config);
    t_log* logger = log_create("worker.log", "Worker", true, LOG_LEVEL_INFO);

    if(config == NULL){
        log_info(logger, "No se inició la configuración del Worker");
    } else {
        log_info(logger, "Se inició la configuración del Worker");
    }

    ip_master = config_get_string_value(config,"IP_MASTER");
    puerto_master = config_get_string_value(config,"PUERTO_MASTER");
    ip_storage = config_get_string_value(config,"IP_STORAGE");
    puerto_storage = config_get_string_value(config,"PUERTO_STORAGE");
    tam_memoria = config_get_int_value(config,"TAM_MEMORIA");
    retardo_memoria = config_get_int_value(config,"RETARDO_MEMORIA");
    algoritmo_reemplazo = config_get_string_value(config,"ALGORITMO_REEMPLAZO");
    path_scripts = config_get_string_value(config,"PATH_SCRIPTS");
    log_level = config_get_string_value(config,"LOG_LEVEL");
    
    espacio_de_usuario = calloc(1, tam_memoria); //Ver si se puede usar calloc. El enunciado menciona malloc. Calloc me limpia en contenido

    conexion_storage = crear_conexion(ip_storage, puerto_storage);
    if(conexion_storage < 0)
	{
		log_error(logger,"No se pudo crear la conexion del Worker al Storage %s:%s", ip_storage, puerto_storage);
		close(conexion_storage);
        log_destroy(logger);
        config_destroy(config);
		exit(1);
	} else {
        log_info(logger, "El Worker se conectó al Storage %s:%s", ip_storage, puerto_storage);
        int size = strlen(id_worker_storage) + 1;
        send(conexion_storage, &size, sizeof(int), 0);
        send(conexion_storage, id_worker_storage, size, 0);
        log_info(logger, "Se envio al storage el Worker ID: %s", id_worker_storage);
    }

    recv(conexion_storage,&tam_bloque,sizeof(int),MSG_WAITALL);
    inicializar_memoria_interna();

    conexion_master = crear_conexion(ip_master, puerto_master);
    if(conexion_master < 0)
    {
        log_error(logger, "No se pudo crear la conexion del Worker al master %s:%s", ip_master, puerto_master);
        close(conexion_master);
        log_destroy(logger);
        config_destroy(config);
        exit(1);
    } else {
        log_info(logger, "El Worker se conectó a Master %s:%s", ip_master, puerto_master);
        enviar_id(conexion_master, id_worker);
        log_info(logger, "Se envio al master el Worker ID: %d", id_worker);
    }

    while(1){
        //Recibe el nombre del archivo
        t_list* recibir_instruccion = recibir_paquete(conexion_master);
        if (recibir_instruccion == NULL) {
        log_warning(logger, "El Master se desconectó. Finalizando Worker...");
        break;
        }

        t_worker_op op = *(t_worker_op*) list_get(recibir_instruccion, 0);

        switch(op){
            case WORKER_LIBRE:
                //Creo una estructura query_worker_h para enviar cuatro parametros al hilo: id_query nombre_archivo, conexion y progCount.
                query_worker_h* query_ejecutando_struct = malloc(sizeof(query_worker_h));
                query_ejecutando_struct->id_query = *(int*) list_get(recibir_instruccion, 1);
                char* archivo_str = strdup(list_get(recibir_instruccion, 2));
                query_ejecutando_struct->archivo = archivo_str;
                query_ejecutando_struct->pc = *(int*) list_get(recibir_instruccion, 3);
                query_ejecutando_struct->desalojar = false;
                
                
                query_ejecutando_struct->fd_w_m = conexion_master;

                log_info(logger, "##Query: %d. Se recibe la Query. El path de operaciones es: %s. PC: %d",query_ejecutando_struct->id_query, query_ejecutando_struct->archivo, query_ejecutando_struct->pc);
                                //Pongo la query que esta ejecutando, en la estructura auxiliar
                pthread_mutex_lock(&mutex_query_ejecutando);
                    query_en_worker = query_ejecutando_struct;
                pthread_mutex_unlock(&mutex_query_ejecutando);

                pthread_t hilo_nueva_query;
                pthread_create(&hilo_nueva_query, NULL, query_interpreter, query_ejecutando_struct);
                pthread_detach(hilo_nueva_query);

                list_destroy_and_destroy_elements(recibir_instruccion, free);
            break;

            case DESALOJO_QUERY:{
                log_info(logger, "##Se recibe orden de desalojo");
                query_worker_h* query_prioridad_struct = malloc(sizeof(query_worker_h));

                query_prioridad_struct->id_query = *(int*) list_get(recibir_instruccion, 1);
                char* archivo_prioridad_str = strdup(list_get(recibir_instruccion, 2));
                query_prioridad_struct->archivo = archivo_prioridad_str;           
                query_prioridad_struct->pc = *(int*) list_get(recibir_instruccion, 3);
                query_prioridad_struct->fd_w_m = conexion_master;
                query_prioridad_struct->desalojar = false;
                
                pthread_mutex_lock(&mutex_query_prioridad);

                    if (query_prioridad) {
                        free(query_prioridad->archivo);
                        free(query_prioridad);
                    }
                query_prioridad = query_prioridad_struct;
                pthread_mutex_unlock(&mutex_query_prioridad);
                
                pthread_mutex_lock(&mutex_query_ejecutando);                
                    if (query_en_worker){
                        query_en_worker->desalojar = true;
                        log_info(logger, "## Query %d: Desalojada por pedido del Master", query_en_worker->id_query);
                    }
                pthread_mutex_unlock(&mutex_query_ejecutando);



                log_info(logger, "##Query: %d. Se recibe la Query. El path de operaciones es: %s.",query_prioridad_struct->id_query, query_prioridad_struct->archivo);
 
                //No creo ningun hilo

                list_destroy_and_destroy_elements(recibir_instruccion, free);
            break;
            }
            case DESALOJO_PASIVO: {
 //               query_worker_h* query_prioridad_struct = malloc(sizeof(query_worker_h));
                
                pthread_mutex_lock(&mutex_query_prioridad);
                if (query_prioridad) {
                    free(query_prioridad->archivo);
                    free(query_prioridad);
                }
                query_prioridad = NULL;
                pthread_mutex_unlock(&mutex_query_prioridad);

                pthread_mutex_lock(&mutex_query_ejecutando);
                if (query_en_worker){
                    query_en_worker->desalojar = true;
                    log_info(logger, "## Desalojo por Desconexión abrupta: Query: %d.", query_en_worker->id_query);
                }
                pthread_mutex_unlock(&mutex_query_ejecutando);
                

                list_destroy_and_destroy_elements(recibir_instruccion, free);

            break;
            }

            default:
            log_warning(logger, "El master envio una Query con %d argumentos", list_size(recibir_instruccion));
            sleep(1);
            break;
        }       
    }

    terminar_worker();

    return 0;
}

void inicializar_memoria_interna(){
    int cantidad_marcos = tam_memoria / tam_bloque;
    marcos = list_create();
    marcos_libres = list_create();
    marcos_ocupados = list_create();
    paginas_presentes = list_create();
    pthread_mutex_init(&mutex_tablas, NULL);
    pthread_mutex_init(&mutex_marcos, NULL);
    pthread_mutex_init(&mutex_espacio_usuario, NULL);
    pthread_mutex_init(&mutex_paginas_presentes, NULL);
    
    for(int i = 0; i < cantidad_marcos; i++){
        t_marco * m = malloc(sizeof(t_marco));
        m->marco = i;
        m->libre = true;
        list_add(marcos,m);
        list_add(marcos_libres,m);
    }
    tablas_paginas = list_create();
    log_info(logger, "Memoria interna inicializada con %d marcos de %d bytes", cantidad_marcos, tam_bloque);
}

void terminar_worker_forzado(int signal){
    int id_query_enviar = -1;
    char* archivo_str_enviar = strdup("NINGUNO");
    int pc_enviar = 0;
    pthread_mutex_lock(&mutex_query_ejecutando);
    if(query_en_worker != NULL){
        id_query_enviar = query_en_worker->id_query;
        free(archivo_str_enviar);
        archivo_str_enviar = strdup(query_en_worker->archivo);
        pc_enviar = query_en_worker->pc;
    }
    pthread_mutex_unlock(&mutex_query_ejecutando);

    t_master_op op_master = DESCONEXION_WORKER;
    t_paquete* respuesta_master = crear_paquete();

    agregar_a_paquete(respuesta_master, &op_master, sizeof(t_master_op));
    agregar_a_paquete(respuesta_master, &id_query_enviar, sizeof(int));
    agregar_a_paquete(respuesta_master, archivo_str_enviar, strlen(archivo_str_enviar) + 1);
    agregar_a_paquete(respuesta_master, &pc_enviar, sizeof(int));

    enviar_paquete(respuesta_master, conexion_master);
    free(archivo_str_enviar);
    eliminar_paquete(respuesta_master);

    t_storage_op op_storage = DESCONEXION;
    t_paquete* respuesta_storage = crear_paquete();
    agregar_a_paquete(respuesta_storage, &op_storage, sizeof(t_storage_op));

    pthread_mutex_lock(&mutex_socket_storage);
    enviar_paquete(respuesta_storage,conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);

    eliminar_paquete(respuesta_storage);

    list_destroy_and_destroy_elements(tablas_paginas, (void*)liberar_tabla_completa);
    list_destroy_and_destroy_elements(marcos, free);
    free(espacio_de_usuario);
    pthread_mutex_destroy(&mutex_tablas);
    pthread_mutex_destroy(&mutex_marcos);
    pthread_mutex_destroy(&mutex_espacio_usuario);
    config_destroy(config);
    log_destroy(logger);
    close(conexion_storage);
    close(conexion_master);
    exit(0);
}

void terminar_worker(){
    close(conexion_storage);
    close(conexion_master);
    //list_destroy_and_destroy_elements(tablas_paginas, free);
    pthread_mutex_lock(&mutex_tablas);
    list_destroy_and_destroy_elements(tablas_paginas, (void*)liberar_tabla_completa);
    pthread_mutex_unlock(&mutex_tablas);
    list_destroy_and_destroy_elements(marcos, free);
    list_destroy_and_destroy_elements(marcos_libres, free);
    list_destroy_and_destroy_elements(marcos_ocupados, free);
    free(espacio_de_usuario);
    pthread_mutex_destroy(&mutex_tablas);
    pthread_mutex_destroy(&mutex_marcos);
    pthread_mutex_destroy(&mutex_espacio_usuario);
    log_info(logger, "##Worker finalizado. Recursos liberados.");
    config_destroy(config);
    log_destroy(logger);
}