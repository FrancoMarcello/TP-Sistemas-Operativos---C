#include "utils.h"

int iniciar_servidor(char* puerto)
{
	int err;
	int socket_storage;
	int opt = 1;

	struct addrinfo hints, *servinfo;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	err = getaddrinfo(NULL, puerto, &hints, &servinfo);
	if (err != 0) {
        fprintf(stderr, "Error en getaddrinfo: %s\n", gai_strerror(err));
        exit(1);
    }

	socket_storage = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
    if (socket_storage < 0) {
        perror("socket");
        freeaddrinfo(servinfo);
        exit(1);
    }

	if (setsockopt(socket_storage, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        freeaddrinfo(servinfo);
        close(socket_storage);
        exit(1);
    }
	
    if (bind(socket_storage, servinfo->ai_addr, servinfo->ai_addrlen) < 0) {
        perror("bind");
        freeaddrinfo(servinfo);
        close(socket_storage);
        exit(1);
    }

    if (listen(socket_storage, SOMAXCONN) < 0) {
        perror("listen");
        freeaddrinfo(servinfo);
        close(socket_storage);
        exit(1);
    }

	freeaddrinfo(servinfo);
	log_trace(logger, "Listo para escuchar a los workers");
	return socket_storage;
}

void* recibir_buffer(int* size, int socket_cliente)
{
	void * buffer;
    int bytes;
	int cod_op;
																		//Añadido
    bytes = recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL); 	//(descartamos cod_op) Lo recibe y no hace nada
    if (bytes <= 0){
        return NULL;
    }

	bytes = recv(socket_cliente, size, sizeof(int), MSG_WAITALL);
    if (bytes <= 0 || *size <= 0) return NULL;
	
	buffer = malloc(*size);
	if (!buffer) return NULL;
	
	bytes = recv(socket_cliente, buffer, *size, MSG_WAITALL);
    if(bytes <= 0){
		free(buffer);
        return NULL;
    }
	return buffer;
}


char* recibir_id(int socket_cliente)
{
    int size;
    int resultado_recv;
    
    // --- 1. Recibir el Tamaño del Buffer (ID) ---
    resultado_recv = recv(socket_cliente, &size, sizeof(int), MSG_WAITALL);
    
    // Validar desconexión o error
    if (resultado_recv <= 0 || size <= 0) return NULL;

    // --- 2. Recibir el Buffer ---
    char* buffer = malloc(size);
    if (!buffer) return NULL;

    resultado_recv = recv(socket_cliente, buffer, size, MSG_WAITALL);
    
    if (resultado_recv <= 0) {
        free(buffer);
        return NULL;
    }

    // --- 3. Crear string NULL-terminated ---
    char* id = malloc(size + 1);
	if (!id) {
        free(buffer);
        return NULL;
    }
	
    memcpy(id, buffer, size);
    id[size] = '\0'; // Asegurar terminador
    
    free(buffer);
    return id;
}

t_storage_op string_a_operacion(const char* op_str)
{
    if (strcmp(op_str, "OP_CREATE_FILE") == 0) return OP_CREATE_FILE;
    if (strcmp(op_str, "OP_TRUNCATE_FILE") == 0) return OP_TRUNCATE_FILE;
    if (strcmp(op_str, "OP_TAG_FILE") == 0) return OP_TAG_FILE;
    if (strcmp(op_str, "OP_COMMIT_TAG") == 0) return OP_COMMIT_TAG;
    if (strcmp(op_str, "OP_WRITE_BLOCK") == 0) return OP_WRITE_BLOCK;
    if (strcmp(op_str, "OP_READ_BLOCK") == 0) return OP_READ_BLOCK;
    if (strcmp(op_str, "OP_DELETE_TAG") == 0) return OP_DELETE_TAG;
    if (strcmp(op_str, "OP_FINISH") == 0) return OP_FINISH;
    return -1; // error
}

t_storage_op recibir_operacion(int socket_cliente)
{
    int opcode;
    
    ssize_t r = recv(socket_cliente, &opcode, sizeof(int), MSG_WAITALL);
    
    if (r == 0) {
        log_info(logger, "Cliente desconectado al recibir operacion");
        return -1;
    }

    if (r < 0) {
        log_error(logger, "Error al recibir operacion del socket %d", socket_cliente);
        return -1;
    }

    log_info(logger, "Operacion recibida: %d", opcode);

    return (t_storage_op) opcode;
}
/*
void* recibir_buffer_storage(int* size, int socket_cliente)
{
	void * buffer;
    if (size <= 0) return NULL; // Validación del tamaño
    
    buffer = malloc(size);
    if (!buffer) return NULL;

    if (recv(socket_cliente, buffer, size, MSG_WAITALL) <= 0) {
        free(buffer);
        return NULL;
    }

    return buffer;
}

t_list* recibir_paquete_storage(int socket_cliente)
{
    int size;
    int desplazamiento = 0;
    void * buffer;
    t_list* valores = list_create();
    int tamanio;

    // primero recibimos tamaño del buffer
    buffer = recibir_buffer_storage(&size, socket_cliente);
    if (!buffer) {
        list_destroy(valores);
        return NULL;
    }

    // parseamos los elementos dentro del buffer
    while(desplazamiento < size)
    {
        memcpy(&tamanio, buffer + desplazamiento, sizeof(int));
        desplazamiento += sizeof(int);

        char* valor = malloc(tamanio);
        if (!valor) {
            for (int i = 0; i < list_size(valores); i++) free(list_get(valores, i));
            list_destroy(valores);
            free(buffer);
            return NULL;
        }
        memcpy(valor, buffer + desplazamiento, tamanio);
        desplazamiento += tamanio;

        list_add(valores, valor);
    }
    free(buffer);
    return valores;
}
*/

t_list* recibir_paquete(int socket_cliente)
{
    int size;
    int desplazamiento = 0;
    void * buffer;
    t_list* valores = list_create();
    int tamanio;

    // primero recibimos tamaño del buffer
    buffer = recibir_buffer(&size, socket_cliente);
    if (!buffer) {
        list_destroy(valores);
        return NULL;
    }

    // parseamos los elementos dentro del buffer
    while(desplazamiento + sizeof(int) <= size)
    {
        memcpy(&tamanio, buffer + desplazamiento, sizeof(int));
        desplazamiento += sizeof(int);

		if (tamanio <= 0 || desplazamiento + tamanio > size) {
        	list_destroy_and_destroy_elements(valores, free);
        	free(buffer);
       		return NULL;
    	}
		
        char* valor = malloc(tamanio + 1);
        memcpy(valor, buffer + desplazamiento, tamanio);
		valor[tamanio] = '\0';
        desplazamiento += tamanio;

        list_add(valores, valor);
    }
    free(buffer);
    return valores;
}


t_config* iniciar_config(char* ruta_config)
{
    t_config* nuevo_config = config_create(ruta_config);
    if (!nuevo_config) {
        fprintf(stderr, "Error al abrir archivo de configuracion: %s\n", ruta_config);
        exit(EXIT_FAILURE);
    }
    return nuevo_config;
}

t_log* iniciar_logger()
{
    if (!config) {
        fprintf(stderr, "Config no inicializada al crear logger\n");
        exit(1);
    }

    char* log_level = config_get_string_value(config, "LOG_LEVEL");
    t_log* nuevo_logger = log_create("storage.log", "Storage", true, log_level_from_string(log_level));
    if (!nuevo_logger) {
        fprintf(stderr, "Error al crear logger\n");
        exit(1);
    }
    return nuevo_logger;
}

void* serializar_paquete(t_paquete* paquete, int bytes)
{
	if (!paquete || !paquete->buffer) return NULL;
	if (paquete->buffer->size > 0 && !paquete->buffer->stream) return NULL;
	
	void * magic = malloc(bytes);
	int desplazamiento = 0;

	memcpy(magic + desplazamiento, &(paquete->codigo_operacion), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(magic + desplazamiento, &(paquete->buffer->size), sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(magic + desplazamiento, paquete->buffer->stream, paquete->buffer->size);
	desplazamiento+= paquete->buffer->size;

	return magic;
}

void enviar_paquete(t_paquete* paquete, int socket_cliente)
{
	int bytes = paquete->buffer->size + 2*sizeof(int);
	void* a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
}
void eliminar_paquete(t_paquete* paquete)
{
	if (!paquete) return;
	free(paquete->buffer->stream);
	free(paquete->buffer);
	free(paquete);
}
void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio)
{
	void* nuevo = realloc(paquete->buffer->stream, paquete->buffer->size + tamanio + sizeof(int));

	if (!nuevo) return;
	
	paquete->buffer->stream = nuevo;

	memcpy(paquete->buffer->stream + paquete->buffer->size, &tamanio, sizeof(int));
	memcpy(paquete->buffer->stream + paquete->buffer->size + sizeof(int), valor, tamanio);

	paquete->buffer->size += tamanio + sizeof(int);
}
t_paquete* crear_paquete(void)
{
	t_paquete* paquete = malloc(sizeof(t_paquete));
	paquete->codigo_operacion = PAQUETE;
	crear_buffer(paquete);
	return paquete;
}

void crear_buffer(t_paquete* paquete)
{
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = 0;
	paquete->buffer->stream = NULL;
}
