#include "utils.h"
#include <commons/collections/list.h>

t_config *config;


void* serializar_paquete(t_paquete* paquete, int bytes)
{
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

int crear_conexion(char *ip, char* puerto)
{
	struct addrinfo hints;
	struct addrinfo *server_info;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	if(getaddrinfo(ip, puerto, &hints, &server_info)<0){
		log_warning(logger, "Error en get addrinfo");
		return -1;
	}

	// Ahora vamos a crear el socket.
	int socket_cliente = socket(server_info->ai_family,server_info->ai_socktype,server_info->ai_protocol);
	if(socket_cliente <0){
		log_warning(logger, "No se creo el el socket");
		return -1;
	}
	// Ahora que tenemos el socket, vamos a conectarlo

//	if(connect(socket_cliente,server_info->ai_addr,server_info->ai_addrlen) == -1){
//			assert(!"no se pudo conectar al server");
//		}
	if(connect(socket_cliente,server_info->ai_addr,server_info->ai_addrlen)<0){
		log_warning(logger, "Error al intentar iniciar una conexión");
		return -1;
	}

	freeaddrinfo(server_info);
	return socket_cliente;
}

void enviar_mensaje(char* mensaje, int socket_cliente)
{
	t_paquete* paquete = malloc(sizeof(t_paquete));

	paquete->codigo_operacion = MENSAJE;
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = strlen(mensaje) + 1;
	paquete->buffer->stream = malloc(paquete->buffer->size);
	memcpy(paquete->buffer->stream, mensaje, paquete->buffer->size);

	int bytes = paquete->buffer->size + 2*sizeof(int);

	void* a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
	eliminar_paquete(paquete);
}


void crear_buffer(t_paquete* paquete)
{
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = 0;
	paquete->buffer->stream = NULL;
}

t_paquete* crear_paquete(void)
{
	t_paquete* paquete = malloc(sizeof(t_paquete));
	paquete->codigo_operacion = PAQUETE;
	crear_buffer(paquete);
	return paquete;
}

void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio)
{
	paquete->buffer->stream = realloc(paquete->buffer->stream, paquete->buffer->size + tamanio + sizeof(int));

	memcpy(paquete->buffer->stream + paquete->buffer->size, &tamanio, sizeof(int));
	memcpy(paquete->buffer->stream + paquete->buffer->size + sizeof(int), valor, tamanio);

	paquete->buffer->size += tamanio + sizeof(int);
}

void enviar_paquete(t_paquete* paquete, int socket_cliente)
{
	int bytes = paquete->buffer->size + 2*sizeof(int);
	void* a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
}

#include <sys/socket.h> // Necesario para send
#include <errno.h>      // Necesario para strerror(errno)

// Usa esta función en ejecutar_flush para enviar el paquete de bloque (OP_WRITE_BLOCK)

void enviar_paquete_grande(t_paquete* paquete, int socket_cliente)
{
    
    int bytes_a_enviar = paquete->buffer->size + 2 * sizeof(int);
    void* a_enviar = serializar_paquete(paquete, bytes_a_enviar);
    int enviados = 0;
    int resultado_send;


    while (enviados < bytes_a_enviar) {
        // Intenta enviar el segmento restante del buffer
        resultado_send = send(socket_cliente, a_enviar+enviados, bytes_a_enviar - enviados,0);

        if (resultado_send == -1) {
            log_error(logger, "CRITICO: Error en send() durante el envio del paquete grande. Error de sistema: %s", strerror(errno)); 
            free(a_enviar);
            return;
        }
        enviados += resultado_send;
    }
    free(a_enviar);
}

void eliminar_paquete(t_paquete* paquete)
{
	free(paquete->buffer->stream);
	free(paquete->buffer);
	free(paquete);
}

void liberar_conexion(int socket_cliente)
{
	close(socket_cliente);
}


// server

// #include"utils.h"

t_log* logger;

int iniciar_servidor(char* puerto)
{

	int socket_servidor;

	struct addrinfo hints, *servinfo;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	if(getaddrinfo(NULL, puerto, &hints, &servinfo)!=0){
		  assert(!"error en getaddrinfo");
	}

	socket_servidor = socket(servinfo->ai_family,servinfo->ai_socktype,servinfo->ai_protocol);
	if(socket_servidor <0){
		  assert(!"error al crear socket");
	}
	int opt = 1;
	setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	if(bind(socket_servidor,servinfo->ai_addr,servinfo->ai_addrlen)<0){
		  assert(!"error bindear");
	}
	if(listen(socket_servidor,SOMAXCONN) == -1)
	{
		  assert(!"error al leer conexiones entrantes");
	}

	freeaddrinfo(servinfo);
	log_info(logger, "Listo para escuchar a mi cliente");

	return socket_servidor;
}

int esperar_cliente(int socket_servidor)
{
	int socket_cliente = accept(socket_servidor,NULL,NULL);
	return socket_cliente;
}

int recibir_operacion(int socket_cliente)
{
	int cod_op;
	if(recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL) > 0)
		return cod_op;
	else
	{
		close(socket_cliente);
		return -1;
	}
}

void* recibir_buffer(int* size, int socket_cliente)
{
	void * buffer;
    int bytes;
	int cod_op;													//Añadido
    bytes = recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL); 	//(descartamos cod_op) Lo recibe y no hace nada
    if (bytes <= 0){
        return NULL;
    }

	bytes = recv(socket_cliente, size, sizeof(int), MSG_WAITALL);
    if(bytes <= 0){
        return NULL;
    }

	buffer = malloc(*size);
	bytes = recv(socket_cliente, buffer, *size, MSG_WAITALL);
    if(bytes <= 0){
        return NULL;
    }
	return buffer;
}




void recibir_mensaje(int socket_cliente)
{
	int size;
	char* buffer = recibir_buffer(&size, socket_cliente);
	log_info(logger, "Me llego el mensaje %s", buffer);
	free(buffer);
}

/*
t_list* recibir_paquete(int socket_cliente)
{
	int size;
	int desplazamiento = 0;
	void * buffer;
	t_list* valores = list_create();
	int tamanio;

	buffer = recibir_buffer(&size, socket_cliente);
	if(!buffer){
		log_warning(logger, "Se recibio un paquete vacío");
		return NULL;
	}
	while(desplazamiento < size)
	{
		memcpy(&tamanio, buffer + desplazamiento, sizeof(int));
		desplazamiento+=sizeof(int);
		char* valor = malloc(tamanio);
		memcpy(valor, buffer+desplazamiento, tamanio);
		desplazamiento+=tamanio;
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


// cosas que agregamos

int conectar(char* path_puerto,char* path_ip){
	char *puerto = config_get_string_value(config,path_puerto);
	char *ip = config_get_string_value(config,path_ip);
	int conexion = crear_conexion(ip,puerto);
    handshake(conexion);
	return conexion;
}

void handshake(int conexion)
{
    int i = 1;
    send(conexion,&i,sizeof(int),0);

    recv(conexion,&i,sizeof(int),MSG_WAITALL);
    if(i == 1){
        log_info(logger,"handshake realizado");
    } else{ 
        log_error(logger,"handshake fallido");
    }
}


void responder_handshake(int conexion)
{
    int i;
    recv(conexion,&i,sizeof(int),MSG_WAITALL);
    send(conexion,&i,sizeof(int),0);
}

uint64_t obtener_tiempo_llegada(){
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (uint64_t) tv.tv_sec * 1000 + tv.tv_usec /1000; //milisegundos  
}