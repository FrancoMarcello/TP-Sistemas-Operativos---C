#include <worker.h>

/*
ver si agregar semaforos en tablas_paginas, marcos, espacio_de_usuario
*/

//int query_id;
int id_query_desalojada;
char* archivo_query_desalojada;
int pc_query_desalojada;
int fd_query_desalojada;
int puntero_clock = 0;


void* query_interpreter(void* arg){
    query_worker_h* path_query = (query_worker_h*) arg;

    //Obtener path completo
    char* path_config = config_get_string_value(config,"PATH_SCRIPTS");
    char path_scripts [256];
    strcpy(path_scripts, path_config);
    char path_completo[512];
    snprintf(path_completo, sizeof(path_completo), "%s%s", path_scripts, path_query->archivo);
    char* path = strdup(path_completo);
    //log_info(logger, "Se ejecutara el path final: %s", path);

    FILE* file = fopen(path, "r");
    log_info(logger, "## Query %d: Se recibe la Query. El archivo es: %s", path_query->id_query, path_query->archivo);

    if(!file){
        log_error(logger, "No se pudo abrir la query: %s", path_query->archivo);
        return NULL;
    }

    int program_counter = path_query->pc;
    char linea[256];

    if(program_counter != 0){
        for (int i = 0; i < path_query->pc; i++) {
            if (!fgets(linea, sizeof(linea), file)) {
            log_warning(logger, "##Query %d: No se pudieron saltar todas las lineas hasta el PC %d", path_query->id_query, path_query->pc);
            break;
            }
        }
    }

    while(fgets(linea, sizeof(linea), file)){
        pthread_mutex_lock(&mutex_query_ejecutando);
        if(query_en_worker==NULL){
            pthread_mutex_unlock(&mutex_query_ejecutando);
            break;
        }
        if(query_en_worker->desalojar){
            int id_query_en_worker = query_en_worker->id_query;
            int pc_query_en_worker = query_en_worker->pc;
            int fd_wm = query_en_worker->fd_w_m;
            // Usamos strdup para COPIAR la cadena del global al heap local
            char* archivo_str = strdup(query_en_worker->archivo);
            pthread_mutex_unlock(&mutex_query_ejecutando);
    
            log_info(logger, "##Query desalojada: %d. Archivo: %s.PC: %d", id_query_en_worker, archivo_str, pc_query_en_worker);

            pthread_mutex_lock(&mutex_tablas);
            for(int i = 0; i < list_size(tablas_paginas); i++){
                t_tabla_paginas* tabla = list_get(tablas_paginas, i);
                bool tiene_modificado = false;

                for(int j = 0; j < list_size(tabla->paginas); j++){
                    t_pagina* p = list_get(tabla->paginas, j);
                    if(p->bit_modificado){
                        tiene_modificado = true;
                        break;
                    }
                }

                if(tiene_modificado){
                    ejecutar_flush(path_query->id_query,tabla->file, tabla->tag);
                }
            }
            pthread_mutex_unlock(&mutex_tablas);

            t_master_op op_master = DESALOJO;
            t_paquete* respuesta_master = crear_paquete();
            agregar_a_paquete(respuesta_master, &op_master, sizeof(t_master_op));
            agregar_a_paquete(respuesta_master, &id_query_en_worker, sizeof(int));
            agregar_a_paquete(respuesta_master, archivo_str, strlen(archivo_str) + 1);
            agregar_a_paquete(respuesta_master, &pc_query_en_worker, sizeof(int));
            enviar_paquete(respuesta_master, fd_wm);
            free(archivo_str);
            eliminar_paquete(respuesta_master);

            free(path_query->archivo);
            free(path_query);
            free(path);
            fclose(file);
            return NULL;
        } else {
            pthread_mutex_unlock(&mutex_query_ejecutando);
        }


        linea[strcspn(linea, "\n")] = 0;                //"Castea" el \n a \0 reemplazando el salto de linea por fin de cadena.
        char* instruccion_str = strtok(linea," ");      //Toma el primer token, " " es el centinela.

        t_instruction instruccion = castear_instruccion(instruccion_str);

        char * parametro_1 = strtok(NULL, " ");
        char * parametro_2 = strtok(NULL, " ");
        char * parametro_3 = strtok(NULL, " ");

        switch(instruccion){
        case CREATE:{
            char* file_name = strtok(parametro_1, ":");
            char* tag = strtok(NULL, ":");
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - CREATE", path_query->id_query, program_counter);
            ejecutar_create(path_query->id_query,file_name, tag);
            log_info(logger, "## Query %d: - Instrucción realizada: CREATE", path_query->id_query);
            break;
        }
        case TRUNCATE:{
            char* file_name = strtok(parametro_1, ":");
            char* tag = strtok(NULL, ":");
            int tam = atoi(parametro_2);
/*          if(tam<=tam_bloque){ 
            tam = tam_bloque;
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - TRUNCATE", path_query->id_query, program_counter);
            ejecutar_truncate(file_name, tag, tam);
            log_info(logger, "## Query %d: - Instrucción realizada: TRUNCATE", path_query->id_query);
            break;
*/
            if (tam % tam_bloque != 0) {
                log_error(logger, "Error: tamaño %d no es multiplo del tamaño de bloque %d", tam, tam_bloque);
                //return;
                break;
            }  
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - TRUNCATE", path_query->id_query, program_counter);
            ejecutar_truncate(path_query->id_query,file_name, tag, tam);
            log_info(logger, "## Query %d: - Instrucción realizada: TRUNCATE", path_query->id_query);
            break;
 
        }
        case WRITE:{
            char* file_name = strtok(parametro_1, ":");
            char* tag = strtok(NULL, ":");
            int dir_base = atoi(parametro_2);
            char * contenido = parametro_3;
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - WRITE", path_query->id_query, program_counter);
            ejecutar_write(path_query->id_query,file_name, tag, dir_base, contenido);
            log_info(logger, "## Query %d: - Instrucción realizada: WRITE", path_query->id_query);
            break;
        }
        case READ:{
            char* file_name = strtok(parametro_1, ":");
            char* tag = strtok(NULL, ":");
            int dir_base = atoi(parametro_2);
            int tam = atoi(parametro_3);
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - READ", path_query->id_query, program_counter);
            ejecutar_read(path_query->id_query,file_name, tag, dir_base, tam);
            log_info(logger, "## Query %d: - Instrucción realizada: READ", path_query->id_query);
            break;
        }
        case TAG:{
            char* file_origen = strtok(parametro_1, ":");
            char* tag_origen = strtok(NULL, ":");
            char* file_destino = strtok(parametro_2, ":");
            char* tag_destino = strtok(NULL, ":");
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - TAG", path_query->id_query, program_counter);
            ejecutar_tag(path_query->id_query,file_origen, tag_origen, file_destino, tag_destino);
            log_info(logger, "## Query %d: - Instrucción realizada: TAG", path_query->id_query);
            break;
        }
        case COMMIT:{
            char* file_name = strtok(parametro_1, ":");
            char* tag = strtok(NULL, ":");
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - COMMIT", path_query->id_query, program_counter);
            ejecutar_commit(path_query->id_query,file_name, tag);
            log_info(logger, "## Query %d: - Instrucción realizada: COMMIT", path_query->id_query);
            break;
        }
        case FLUSH:{
            char* file_name = strtok(parametro_1, ":");
            char* tag = strtok(NULL, ":");
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - FLUSH", path_query->id_query, program_counter);
            ejecutar_flush(path_query->id_query,file_name, tag);
            log_info(logger, "## Query %d: - Instrucción realizada: FLUSH", path_query->id_query);
            break;
        }
        case DELETE:{
            char* file_name = strtok(parametro_1, ":");
            char* tag = strtok(NULL, ":");
            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - DELETE", path_query->id_query, program_counter);
            ejecutar_delete(path_query->id_query,file_name, tag);
            log_info(logger, "## Query %d: - Instrucción realizada: DELETE", path_query->id_query);
            break;
        }
        case END:{
            pthread_mutex_lock(&mutex_tablas);
            for(int i = 0; i < list_size(tablas_paginas); i++){
                t_tabla_paginas* tabla = list_get(tablas_paginas, i);
                bool tiene_modificado = false;

                for(int j = 0; j < list_size(tabla->paginas); j++){
                    t_pagina* p = list_get(tabla->paginas, j);
                    if(p->bit_modificado){
                        tiene_modificado = true;
                        break;
                    }
                }

                if(tiene_modificado){
                    ejecutar_flush(path_query->id_query,tabla->file, tabla->tag);
                }
            }

            pthread_mutex_unlock(&mutex_tablas);

            pthread_mutex_lock(&mutex_query_prioridad);
                query_prioridad = NULL;
            pthread_mutex_unlock(&mutex_query_prioridad);            
            
            pthread_mutex_lock(&mutex_query_ejecutando);
                query_en_worker = NULL;
            pthread_mutex_unlock(&mutex_query_ejecutando);

            free(path_query->archivo);
            free(path);
            fclose(file);

            t_master_op op = FIN;
            t_paquete* respuesta_master = crear_paquete();
            agregar_a_paquete(respuesta_master, &op, sizeof(t_master_op));
            agregar_a_paquete(respuesta_master, &(path_query->id_query), sizeof(int));
            char* archivo = strdup(path_query->archivo);
            agregar_a_paquete(respuesta_master, archivo, strlen(archivo) + 1);

            log_info(logger, "##Query %d: FETCH - Program Counter:  %d - END", path_query->id_query, program_counter);
            enviar_paquete(respuesta_master, path_query->fd_w_m);
            eliminar_paquete(respuesta_master);
            log_info(logger, "## Query %d: - Instrucción realizada: END", path_query->id_query);
            free(archivo);
            free(path_query);
            return NULL;
        }
            
        default:   
            log_info(logger, "Instrucción no detectada. Fallo");
            break;
    }
        
        program_counter++;
        pthread_mutex_lock(&mutex_query_ejecutando);
        if(query_en_worker != NULL)
        query_en_worker->pc=program_counter;
        pthread_mutex_unlock(&mutex_query_ejecutando);
       
    }
    free(path_query->archivo);
    free(path_query);
    free(path);
    fclose(file);
    return NULL;
}

void ejecutar_create(int id, char* file, char* tag){
    t_storage_op operacion = OP_CREATE_FILE; 
    //enviar_operacion(conexion_storage,operacion);
    t_paquete* paquete = crear_paquete();
    agregar_a_paquete(paquete, &operacion, sizeof(t_storage_op));
    agregar_a_paquete(paquete,file,strlen(file)+1);
    agregar_a_paquete(paquete,tag,strlen(tag)+1);
    int tamanio = 0;
    agregar_a_paquete(paquete,&tamanio,sizeof(int));
    agregar_a_paquete(paquete,&id,sizeof(int));
    pthread_mutex_lock(&mutex_socket_storage);
    enviar_paquete(paquete,conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);
    eliminar_paquete(paquete);
}

void ejecutar_truncate(int id, char* file, char* tag, int tamanio){
    t_storage_op operacion = OP_TRUNCATE_FILE;
    //enviar_operacion(conexion_storage,operacion);
    t_paquete* paquete = crear_paquete();
    agregar_a_paquete(paquete, &operacion, sizeof(t_storage_op));
    agregar_a_paquete(paquete,file,strlen(file)+1);
    agregar_a_paquete(paquete,tag,strlen(tag)+1);
    agregar_a_paquete(paquete,&tamanio,sizeof(int));
    agregar_a_paquete(paquete,&id,sizeof(int));
    pthread_mutex_lock(&mutex_socket_storage);
    enviar_paquete(paquete,conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);
    eliminar_paquete(paquete);
}

void ejecutar_write(int id, char* file, char* tag, int dir_base, char* contenido) {
    int bytes_restantes = strlen(contenido);
    int bytes_escritos = 0;
    int pagina_actual = dir_base / tam_bloque;
    int offset = dir_base % tam_bloque;



    while (bytes_restantes > 0) {
        int espacio_disponible = tam_bloque - offset;
        int bytes_a_escribir = (bytes_restantes < espacio_disponible) ? bytes_restantes : espacio_disponible;

        pthread_mutex_lock(&mutex_tablas);
        t_tabla_paginas* tabla = obtener_tabla(file, tag);
        if (!tabla) {
            tabla = malloc(sizeof(t_tabla_paginas));
            tabla->file = strdup(file);
            tabla->tag = strdup(tag);
            tabla->paginas = list_create();
            list_add(tablas_paginas, tabla);
        }

        t_pagina* pagina = obtener_pagina(tabla, pagina_actual);
        if (!pagina) {
            pagina = malloc(sizeof(t_pagina));
            pagina->numero_pagina = pagina_actual;
            pagina->bit_presencia = false;
            pagina->bit_uso = false;
            pagina->bit_modificado = false;
            pagina->marco = -1;
            list_add(tabla->paginas, pagina);
        }

        if (!pagina->bit_presencia) {
            pthread_mutex_lock(&mutex_paginas_presentes);
            t_marco* m = asignar_marco();

            if (!m) {
                log_warning(logger, "No hay marcos libres para realizar WRITE");
                t_pagina* victima;
                if(strcmp(algoritmo_reemplazo, "LRU") == 0) {
                     victima = algoritmo_lru();
                } else {
                    victima = algoritmo_clock(); // Tu ELSE está acá
                }
                if (victima) {
                    t_tabla_paginas* tabla_victima = obtener_tabla_por_pagina(victima);
                    if (victima->bit_modificado && tabla_victima) {
                        flush_a_storage(victima,id, tabla_victima->file, tabla_victima->tag);
                    }
                    log_info(logger,"## Query %d: Se reemplaza la página %s:%s/%d por %s:%s/%d",id, tabla_victima->file, tabla_victima->tag, victima->numero_pagina,file, tag, pagina_actual);
                    int marco_aux = victima->marco; //Agregado porque no encontraba marcos
                    liberar_marco(marco_aux);
                    log_info(logger,"## Query %d: Se libera el Marco: %d, perteneciente al - File: %s - Tag: %s ",id ,victima->marco,file, tag);
                    victima->bit_presencia = false;
                    victima->marco = -1;
                    list_remove_element(paginas_presentes, victima);
                }
                m = asignar_marco();
            }
            pagina->marco = m->marco;
            pagina->bit_presencia = true;
            m->libre = false;
            list_add(paginas_presentes,pagina);
            pthread_mutex_unlock(&mutex_paginas_presentes);

            log_info(logger, "Query %d: - Memoria Add - File: %s - Tag: %s - Pagina: %d - Marco: %d",id , file, tag, pagina->numero_pagina, pagina->marco);

            log_info(logger, "Query %d: Se asigna el Marco: %d a la Pagina: %d - File: %s - Tag: %s", id , pagina->marco, pagina->numero_pagina, file, tag);
        }

        char* direccion_fisica = (char*)espacio_de_usuario + (pagina->marco * tam_bloque) + offset;
        pthread_mutex_lock(&mutex_espacio_usuario);
        memcpy(direccion_fisica, contenido + bytes_escritos, bytes_a_escribir);
        log_info(logger, "Query %d: Accion: ESCRIBIR - Direccion Fisica: %p - Valor: %.*s",id , direccion_fisica, bytes_a_escribir, contenido + bytes_escritos);
        pthread_mutex_unlock(&mutex_espacio_usuario);
        usleep(retardo_memoria * 1000);

        pthread_mutex_lock(&mutex_paginas_presentes);
        pagina->bit_modificado = true;
        pagina->bit_uso = true;
        pagina->ultima_referencia = obtener_timestamp_ms();
        pthread_mutex_unlock(&mutex_paginas_presentes);
        pthread_mutex_unlock(&mutex_tablas);


        bytes_restantes -= bytes_a_escribir;
        bytes_escritos += bytes_a_escribir;
        pagina_actual++;
        offset = 0;
    }

 }

void ejecutar_read(int id, char* file, char* tag, int dir_base, int tamanio) {
//  usleep(retardo_memoria * 1000);

  int bytes_restantes = tamanio;
  int bytes_leidos = 0;
  int pagina_actual = dir_base / tam_bloque;
  int offset = dir_base % tam_bloque;

  char* buffer_lectura = malloc(tamanio + 1);
  if (!buffer_lectura) {
    log_error(logger, "Query %d: Error de alloc buffer_lectura", id);
    return;
  }

  memset(buffer_lectura, 0, tamanio + 1);

  while (bytes_restantes > 0) {
    int espacio_disponible = tam_bloque - offset;
    int bytes_a_leer = (bytes_restantes < espacio_disponible) ? bytes_restantes : espacio_disponible;

  pthread_mutex_lock(&mutex_tablas);
    t_tabla_paginas* tabla = obtener_tabla(file, tag);
    if (!tabla) {
      log_info(logger, "Query %d: - Memoria Miss PageFault (TP). Creando Tabla - File: %s - Tag: %s", id, file, tag);
      tabla = malloc(sizeof(t_tabla_paginas));
      tabla->file = strdup(file);
      tabla->tag = strdup(tag);
      tabla->paginas = list_create();
      list_add(tablas_paginas, tabla);
            
    }

    t_pagina* pagina = obtener_pagina(tabla, pagina_actual);
    if (!pagina || !pagina->bit_presencia) {
      log_info(logger, "Query %d: - Memoria Miss PageFault - File: %s - Tag: %s - Pagina: %d",id , file, tag, pagina_actual);
            if (!pagina) {
                pagina = malloc(sizeof(t_pagina));
                pagina->numero_pagina = pagina_actual;
                pagina->bit_presencia = false;
                pagina->bit_uso = false;
                pagina->bit_modificado = false;
                pagina->marco = -1;
                list_add(tabla->paginas, pagina);
            }
      pthread_mutex_unlock(&mutex_tablas);            
      cargar_pagina_desde_storage(tabla, pagina,id , file, tag, pagina_actual);
    } else {
        pthread_mutex_unlock(&mutex_tablas);
    }

    char* direccion_fisica = (char*)espacio_de_usuario + (pagina->marco * tam_bloque) + offset;
    pthread_mutex_lock(&mutex_espacio_usuario);
    memcpy(buffer_lectura + bytes_leidos, direccion_fisica, bytes_a_leer);
    log_info(logger, "Query %d: Accion: LEER - Direccion Fisica: %p - Valor: %.*s",id , direccion_fisica, bytes_a_leer, buffer_lectura + bytes_leidos);
    pthread_mutex_unlock(&mutex_espacio_usuario);
    usleep(retardo_memoria * 1000);

    pthread_mutex_lock(&mutex_paginas_presentes);
    pagina->bit_uso = true;
    pagina->ultima_referencia = obtener_timestamp_ms();
    pthread_mutex_unlock(&mutex_paginas_presentes);


    bytes_restantes -= bytes_a_leer;
    bytes_leidos += bytes_a_leer;
    pagina_actual++;
    offset = 0;
  }



  buffer_lectura[tamanio] = '\0';

  t_master_op op = LECTURA;
  t_paquete* paquete_respuesta = crear_paquete();
  agregar_a_paquete(paquete_respuesta, &op, sizeof(t_master_op));
  agregar_a_paquete(paquete_respuesta, &id, sizeof(int));
  agregar_a_paquete(paquete_respuesta, buffer_lectura, strlen(buffer_lectura) + 1);
  agregar_a_paquete(paquete_respuesta,file,strlen(file)+1);
    agregar_a_paquete(paquete_respuesta,tag,strlen(tag)+1);
  enviar_paquete(paquete_respuesta, conexion_master);
  eliminar_paquete(paquete_respuesta);

  log_info(logger, "Query %d: Lectura completada - Valor: %s",id , buffer_lectura);

  free(buffer_lectura);
}

void liberar_tabla_completa(t_tabla_paginas* tabla) {
    if (tabla == NULL) return;
    free(tabla->file);
    free(tabla->tag);
    list_destroy_and_destroy_elements(tabla->paginas, free); 
    free(tabla);
}

void ejecutar_tag(int id, char* file_origen, char* tag_origen, char* file_destino, char* tag_destino){
    t_storage_op operacion = OP_TAG_FILE;
    //enviar_operacion(conexion_storage,operacion);
    t_paquete* paquete = crear_paquete();
    agregar_a_paquete(paquete, &operacion, sizeof(t_storage_op));
    agregar_a_paquete(paquete,file_origen,strlen(file_origen)+1);
    agregar_a_paquete(paquete,tag_origen,strlen(tag_origen)+1);
    agregar_a_paquete(paquete,file_destino,strlen(file_destino)+1);
    agregar_a_paquete(paquete,tag_destino,strlen(tag_destino)+1);
    agregar_a_paquete(paquete,&id,sizeof(int));
    pthread_mutex_lock(&mutex_socket_storage);
    enviar_paquete(paquete,conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);
    eliminar_paquete(paquete);
}

void ejecutar_commit(int id, char* file, char* tag){

    ejecutar_flush(id, file, tag); // se ejecuta de forma implicita

    t_storage_op operacion = OP_COMMIT_TAG;
    //enviar_operacion(conexion_storage,operacion);
    t_paquete* paquete = crear_paquete();
    agregar_a_paquete(paquete, &operacion, sizeof(t_storage_op));
    agregar_a_paquete(paquete,file,strlen(file)+1);
    agregar_a_paquete(paquete,tag,strlen(tag)+1);
    agregar_a_paquete(paquete,&id,sizeof(int));
    pthread_mutex_lock(&mutex_socket_storage);
    enviar_paquete(paquete,conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);
    eliminar_paquete(paquete);
}

void ejecutar_flush(int id, char* file, char* tag){
    usleep(retardo_memoria * 1000);

    t_tabla_paginas* tabla = obtener_tabla(file,tag);
    if(!tabla){
        log_error(logger, "No existe tabla de paginas para File: %s - Tag: %s",file,tag);
        return;
    }

    for(int i = 0; i < list_size(tabla->paginas); i++){
        t_pagina* p = list_get(tabla->paginas,i);
        if(p->bit_presencia && p->bit_modificado){
            char* direccion_fisica = (char*)espacio_de_usuario + (p->marco * tam_bloque);

            pthread_mutex_lock(&mutex_espacio_usuario);
            char * buffer = malloc(tam_bloque);
            memcpy(buffer,direccion_fisica,tam_bloque);
            pthread_mutex_unlock(&mutex_espacio_usuario);
            usleep(retardo_memoria * 1000);

            log_info(logger, "Query: FLUSH - Persistiendo File: %s - Tag: %s - Pagina: %d - Marco: %d",file, tag, p->numero_pagina, p->marco);

            t_storage_op operacion = OP_WRITE_BLOCK;
            //enviar_operacion(conexion_storage,operacion);
            t_paquete* paquete = crear_paquete();
            agregar_a_paquete(paquete, &operacion, sizeof(t_storage_op));
            agregar_a_paquete(paquete, file, strlen(file) + 1);
            agregar_a_paquete(paquete, tag, strlen(tag) + 1);
            agregar_a_paquete(paquete, &(p->numero_pagina), sizeof(int));
            agregar_a_paquete(paquete, buffer, tam_bloque);
            agregar_a_paquete(paquete,&id,sizeof(int));
            pthread_mutex_lock(&mutex_socket_storage);    
            enviar_paquete(paquete,conexion_storage);
            pthread_mutex_unlock(&mutex_socket_storage);    
            eliminar_paquete(paquete);

            p->bit_modificado = false;
            free(buffer);
        }
    }
    log_info(logger, "## Query - Instruccion realizada: FLUSH - File: %s - Tag: %s", file, tag);
}

void ejecutar_delete(int id, char* file, char* tag){
    t_storage_op operacion = OP_DELETE_TAG;
    //enviar_operacion(conexion_storage,operacion);
    t_paquete* paquete = crear_paquete();
    agregar_a_paquete(paquete, &operacion, sizeof(t_storage_op));
    agregar_a_paquete(paquete,file,strlen(file)+1);
    agregar_a_paquete(paquete,tag,strlen(tag)+1);
    agregar_a_paquete(paquete,&id,sizeof(int));
    pthread_mutex_lock(&mutex_socket_storage);    
    enviar_paquete(paquete,conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);    
    eliminar_paquete(paquete);
}

void ejecutar_unknow(){
    log_info(logger, "instrucción desconocida");
}

void enviar_id(int conexion, int id_worker){
    send(conexion,&id_worker,sizeof(int),0);
    int id_confirmado;
    recv(conexion,&id_confirmado,sizeof(int),MSG_WAITALL);
    if(id_confirmado != 1){
        log_error(logger,"handshake de worker fallido");
    }
}

t_instruction castear_instruccion(char* instruccion){
    if(strcmp(instruccion, "CREATE") == 0) return CREATE;
    if(strcmp(instruccion, "TRUNCATE") == 0)  return TRUNCATE;
    if(strcmp(instruccion, "WRITE") == 0)  return WRITE;
    if(strcmp(instruccion, "READ") == 0)  return READ;
    if(strcmp(instruccion, "TAG") == 0)  return TAG;
    if(strcmp(instruccion, "COMMIT") == 0)  return COMMIT;
    if(strcmp(instruccion, "FLUSH") == 0)  return FLUSH;
    if(strcmp(instruccion, "DELETE") == 0)  return DELETE;
    if(strcmp(instruccion, "END") == 0)  return END;
    return UNKNOW;
}


void enviar_operacion(int socket, t_storage_op operacion){
    int size = sizeof(t_storage_op);

    if (send(socket, &size, sizeof(int), 0) <= 0) {
        log_error(logger, "Error enviando tamaño de la operación");
        return;
    }

    if (send(socket, &operacion, size, 0) <= 0) {
        log_error(logger, "Error enviando la operación");
        return;
    }
}

t_tabla_paginas* obtener_tabla(char* file,char* tag){
    for(int i = 0; i < list_size(tablas_paginas); i++){
        t_tabla_paginas* t = list_get(tablas_paginas,i);
        if(strcmp(t->file,file) == 0 && strcmp(t->tag,tag) == 0) return t;
    }
    return NULL;
}

t_pagina* obtener_pagina(t_tabla_paginas* tabla, int pagina_buscar){
    for(int i = 0; i < list_size(tabla->paginas); i++){
        t_pagina* p = list_get(tabla->paginas, i);
        if(p->numero_pagina == pagina_buscar) return p;
    }
    return NULL;
}

t_marco* asignar_marco() { // ver si agregar semaforo
    pthread_mutex_lock(&mutex_marcos);
    if (list_is_empty(marcos_libres)){
        pthread_mutex_unlock(&mutex_marcos);
        return NULL;
    }
    t_marco* m = list_remove(marcos_libres, 0);
    m->libre = false;
    list_add(marcos_ocupados, m);
    pthread_mutex_unlock(&mutex_marcos);
    return m;
}

t_pagina* algoritmo_lru() {
    t_pagina* victima = NULL;
    long ultima_mas_vieja = LONG_MAX;
    //pthread_mutex_lock(&mutex_tablas);
    // descomentar para ver el tiempo de acceso de cada una antes del remplazo
   /*
   t_list *paginas_presentes = list_create();
    for (int i = 0; i < list_size(tablas_paginas); i++) {
        t_tabla_paginas *tabla = list_get(tablas_paginas, i);
        for (int j = 0; j < list_size(tabla->paginas); j++) {
            t_pagina *pagina = list_get(tabla->paginas, j);
            if (pagina->bit_presencia) list_add(paginas_presentes, pagina);
        }
    }

    if (list_is_empty(paginas_presentes)) {
        pthread_mutex_unlock(&mutex_tablas);
        log_error(logger, "LRU: no hay paginas presentes en memoria");
        list_destroy(paginas_presentes);
        return NULL;
    }

    int n = list_size(paginas_presentes);
    for(int i = 0; i < n; i++){
        t_pagina * n = list_get(paginas_presentes,i);
        log_warning(logger,"Marco: %d - Pagina: %d - Acceso: %ld",n->marco,n->numero_pagina,n->ultima_referencia);
    }
    */
         for (int i = 0; i < list_size(paginas_presentes); i++) {
            t_pagina* pagina = list_get(paginas_presentes, i);
            if (!pagina->bit_presencia || pagina->marco < 0) continue;
            if (victima == NULL || pagina->ultima_referencia < ultima_mas_vieja) {
                ultima_mas_vieja = pagina->ultima_referencia;
                victima = pagina;
            }
        }

    //pthread_mutex_unlock(&mutex_tablas);

    if (victima != NULL)
        log_info(logger, "LRU: pagina victima seleccionada -> Marco: %d | Pagina: %d | Ultima ref: %ld",victima->marco, victima->numero_pagina, victima->ultima_referencia);
    else
        log_error(logger, "LRU: No se encontro ninguna pagina valida para reemplazar.");

    return victima;
}

void liberar_marco(int numero_marco) {
if(numero_marco==-1){
    log_error(logger, "Error: Intento de liberar marco -1");
    return;
}
    pthread_mutex_lock(&mutex_marcos);
    for (int i = 0; i < list_size(marcos); i++) {
        t_marco* marco = list_get(marcos, i);
        if (marco->marco == numero_marco) {
            marco->libre = true;
            list_add(marcos_libres,marco);
            list_remove_element(marcos_ocupados, marco);
            pthread_mutex_unlock(&mutex_marcos);
            return;
        }
    }
    pthread_mutex_unlock(&mutex_marcos);
    log_error(logger, "Error: intento de liberar marco inexistente (%d)", numero_marco);
}

void flush_a_storage(t_pagina* pagina_victima,int id,  char* file, char* tag) {
    if (!pagina_victima || pagina_victima->marco == -1)
        return;

    char* direccion_fisica = (char*)espacio_de_usuario + (pagina_victima->marco * tam_bloque);
    t_storage_op operacion = OP_WRITE_BLOCK;
    t_paquete* paquete = crear_paquete();
    agregar_a_paquete(paquete, &operacion, sizeof(t_storage_op));
    agregar_a_paquete(paquete, file, strlen(file) + 1);
    agregar_a_paquete(paquete, tag, strlen(tag) + 1);
    agregar_a_paquete(paquete, &pagina_victima->numero_pagina, sizeof(int));
    agregar_a_paquete(paquete, direccion_fisica, tam_bloque);
    agregar_a_paquete(paquete,&id ,sizeof(int));
    pthread_mutex_lock(&mutex_socket_storage);
    enviar_paquete(paquete, conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);    
    eliminar_paquete(paquete);

    log_info(logger,"Query: FLUSH - Persistiendo File: %s - Tag: %s - Pagina: %d - Marco: %d",file, tag, pagina_victima->numero_pagina, pagina_victima->marco);

    pagina_victima->bit_modificado = false;
}

t_tabla_paginas* obtener_tabla_por_pagina(t_pagina* pagina_victima) {
    for (int i = 0; i < list_size(tablas_paginas); i++) {
        t_tabla_paginas* tabla = list_get(tablas_paginas, i);
        for (int j = 0; j < list_size(tabla->paginas); j++) {
            t_pagina* p = list_get(tabla->paginas, j);
            if (p == pagina_victima)
                return tabla;
        }
    }
    return NULL;
}

void cargar_pagina_desde_storage(t_tabla_paginas* tabla, t_pagina* pagina,int id,  char* file, char* tag, int numero_pagina){
    t_storage_op op = OP_READ_BLOCK;
    t_paquete* paquete = crear_paquete();
    agregar_a_paquete(paquete, &op, sizeof(t_storage_op));
    agregar_a_paquete(paquete, file, strlen(file)+1);
    agregar_a_paquete(paquete, tag, strlen(tag)+1);
    agregar_a_paquete(paquete, &numero_pagina, sizeof(int));
    agregar_a_paquete(paquete, &id, sizeof(int));
    pthread_mutex_lock(&mutex_socket_storage);    
    enviar_paquete(paquete,conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);    
    eliminar_paquete(paquete);

    pthread_mutex_lock(&mutex_socket_storage);
    t_list* respuesta = recibir_paquete(conexion_storage);
    pthread_mutex_unlock(&mutex_socket_storage);

    char* contenido_bloque = list_get(respuesta, 0);

    if (respuesta == NULL) {
        log_error(logger, "Error critico: La conexión con Storage se perdió o la respuesta fue nula.");
        return; 
    }
    
    if (list_is_empty(respuesta) || list_size(respuesta) < 1) {
        log_error(logger, "Error: Storage no devolvió contenido de bloque para el READ.");
        list_destroy(respuesta); // Destruir la lista vacía si tu implementación lo permite
        return;
    }

    log_info(logger, "La respuesta de Storage es: %s",contenido_bloque);

    t_marco* m = asignar_marco();
    if(!m){
        log_warning(logger, "No hay marcos libres para cargar pagina desde storage (READ)");
        pthread_mutex_lock(&mutex_tablas);
        pthread_mutex_lock(&mutex_paginas_presentes);

        if(strcmp(algoritmo_reemplazo,"LRU") == 0){
            //pthread_mutex_lock(&mutex_paginas_presentes);
            t_pagina* victima = algoritmo_lru();
            //pthread_mutex_unlock(&mutex_paginas_presentes);
            if (victima) {
                //pthread_mutex_lock(&mutex_tablas);
                t_tabla_paginas* tabla_victima = obtener_tabla_por_pagina(victima);
                if (victima->bit_modificado && tabla_victima) {
                    log_info(logger, "Realizando FLUSH");
                    flush_a_storage(victima,id , tabla_victima->file, tabla_victima->tag);
                }
                log_info(logger,"## Query %d: Se reemplaza la página %s:%s/%d por %s:%s/%d",id, tabla_victima->file, tabla_victima->tag, victima->numero_pagina,file, tag, numero_pagina);
                liberar_marco(victima->marco);
                log_info(logger,"## Query %d: Se libera el Marco: %d, perteneciente al - File: %s - Tag: %s ",id ,victima->marco,file, tag);
                victima->bit_presencia = false;
                victima->marco = -1;
                //pthread_mutex_unlock(&mutex_tablas);

                list_remove_element(paginas_presentes, victima);
            }


        }else{
            //pthread_mutex_lock(&mutex_paginas_presentes);
            t_pagina* victima = algoritmo_clock();
            //pthread_mutex_unlock(&mutex_paginas_presentes);
            if(victima){
                //pthread_mutex_lock(&mutex_tablas);
                t_tabla_paginas* tabla_victima = obtener_tabla_por_pagina(victima);
                if (victima->bit_modificado && tabla_victima) {
                    flush_a_storage(victima,id , tabla_victima->file, tabla_victima->tag);
                }
                log_info(logger,"## Query %d: Se reemplaza la página %s:%s/%d por %s:%s/%d",id, tabla_victima->file, tabla_victima->tag, victima->numero_pagina,file, tag, numero_pagina);
                liberar_marco(victima->marco);
                log_info(logger,"## Query %d: Se libera el Marco: %d, perteneciente al - File: %s - Tag: %s ",id ,victima->marco,file, tag);
                victima->bit_presencia = false;
                victima->marco = -1;
                //pthread_mutex_unlock(&mutex_tablas);

                //pthread_mutex_lock(&mutex_paginas_presentes);
                list_remove_element(paginas_presentes, victima);
                //pthread_mutex_unlock(&mutex_paginas_presentes);
            }
        }
        pthread_mutex_unlock(&mutex_paginas_presentes);
        pthread_mutex_unlock(&mutex_tablas);

        m = asignar_marco();
        if(m == NULL){
            log_warning(logger, "No se pudo reasignar el marco");
            return; //Se debe manejar el error de otra forma
        }
    }

    pthread_mutex_lock(&mutex_espacio_usuario);
    char* direccion_fisica = (char*)espacio_de_usuario + (m->marco * tam_bloque);
    memcpy(direccion_fisica, contenido_bloque, tam_bloque);
    pthread_mutex_unlock(&mutex_espacio_usuario);
    usleep(retardo_memoria * 1000);

    pagina->bit_presencia = true;
    pagina->bit_modificado = false;
    pagina->bit_uso = true;
    pagina->ultima_referencia = obtener_timestamp_ms();
    pagina->marco = m->marco;
    m->libre = false;

    pthread_mutex_lock(&mutex_paginas_presentes);
    list_add(paginas_presentes,pagina);
    pthread_mutex_unlock(&mutex_paginas_presentes);

    log_info(logger, "Query %d: Se asigna el Marco: %d a la Pagina: %d - File: %s - Tag: %s", id, pagina->marco, pagina->numero_pagina, file, tag);

    log_info(logger, "Query %d: - Memoria Add - File: %s - Tag: %s - Pagina: %d - Marco: %d", id, file, tag, numero_pagina, m->marco);

    list_destroy_and_destroy_elements(respuesta, free);
}

t_pagina * algoritmo_clock() {
    //pthread_mutex_lock(&mutex_tablas);

    int n = list_size(paginas_presentes);
    t_pagina *victima = NULL;
    bool encontrado = false;
    int intentos = 0;

    list_sort(paginas_presentes, (void*) comparar_paginas_por_marco);

    // descomentar para ver los marcos con sus paginas a remplazar
    /*log_warning(logger,"LOS MARCOS CON LAS PAGINAS SON:");
    for(int i = 0; i < n; i++){
        t_pagina * n = list_get(paginas_presentes,i);
        log_info(logger,"Marco: %d - Pagina: %d - Presencia: %d - Uso: %d - Modificado: %d",n->marco,n->numero_pagina,n->bit_presencia,n->bit_uso,n->bit_modificado);
    }*/

    while (!encontrado && intentos < 3) {

        // primera pasada U=0 && M=0
        for (int i = 0; i < n; i++) {
            int index = (puntero_clock + i) % n;
            t_pagina *p = list_get(paginas_presentes, index);
            if (!p->bit_presencia || p->marco < 0) continue;
            if (!p->bit_uso && !p->bit_modificado) {
                victima = p;
                puntero_clock = (index + 1) % n;
                encontrado = true;
                break;
            }
        }
        if (encontrado) break;

        // segunda pasada U=0 && M=1
        for (int i = 0; i < n; i++) {
            int index = (puntero_clock + i) % n;
            t_pagina *p = list_get(paginas_presentes, index);
            if (!p->bit_presencia || p->marco < 0) continue;
            if (!p->bit_uso && p->bit_modificado) {
                victima = p;
                puntero_clock = (index + 1) % n;
                encontrado = true;
                break;
            } else {
                p->bit_uso = false;
            }
        }

        intentos++;
    }
    //pthread_mutex_unlock(&mutex_tablas);
    if (victima) {
        log_info(logger, "CLOCK-M: pagina victima seleccionada -> Marco %d | Pagina %d | U=%d | M=%d",victima->marco, victima->numero_pagina, victima->bit_uso, victima->bit_modificado);
    } else {
        log_error(logger, "CLOCK-M: no se encontro victima tras las pasadas");
        return NULL;
    }

    return victima;
}

bool comparar_paginas_por_marco(t_pagina* a, t_pagina* b) {
    return a->marco < b->marco;
}

long obtener_timestamp_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
}