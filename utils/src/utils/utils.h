#ifndef UTILS_H_
#define UTILS_H_

#include<stdio.h>
#include<stdlib.h>
#include<signal.h>
#include<unistd.h>
#include<sys/socket.h>
#include<netdb.h>
#include<string.h>
#include<commons/log.h>
#include<commons/collections/list.h>
#include<assert.h>
#include<commons/config.h>
#include <signal.h>
#include <sys/time.h>

#define PUERTO "4444"

typedef enum
{
	MENSAJE,
	PAQUETE
}op_code;

typedef enum
{
	CREATE,
	TRUNCATE,
	WRITE,
	READ,
	TAG,
	COMMIT,
	FLUSH,
	DELETE,
	END,
	UNKNOW
} t_instruction;

typedef struct
{
	int size;
	void* stream;
} t_buffer;

typedef struct
{
	op_code codigo_operacion;
	t_buffer* buffer;
} t_paquete;

typedef struct{
	int id;
	int pc;
    char* archivo;
    int prioridad;
    int fd_query;
	uint64_t tiempo_llegada;
	pthread_t hilo_aging;
	bool aging_activo;
	bool en_desalojo;
} query;

typedef struct{
	int id_query;
	char* archivo;
	int fd_w_m;
	int pc;
	volatile bool desalojar;
} query_worker_h;


typedef struct
{
    int fd;
    int id;
	query* id_query;	//No hacerle Free. 
} worker;

int crear_conexion(char* ip, char* puerto);
void enviar_mensaje(char* mensaje, int socket_cliente);
t_paquete* crear_paquete(void);
void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio);
void enviar_paquete(t_paquete* paquete, int socket_cliente);
void enviar_paquete_grande(t_paquete* paquete, int socket_cliente);
void liberar_conexion(int socket_cliente);
void eliminar_paquete(t_paquete* paquete);
//
// cosas agregadas
//
//



extern t_log* logger;
extern t_config* config;

void* recibir_buffer(int*, int);

int iniciar_servidor(char* puerto);
int esperar_cliente(int);
t_list* recibir_paquete(int);
void recibir_mensaje(int);
int recibir_operacion(int);

void handshake(int conexion);
int conectar(char* path_puerto,char* path_ip);
void responder_handshake(int conexion);
uint64_t obtener_tiempo_llegada();
#endif /* UTILS_H_ */
