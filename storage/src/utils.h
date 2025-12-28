#ifndef UTILS_H_
#define UTILS_H_
#define _GNU_SOURCE

//estandar c
#include <stdio.h>          // printf, perror, FILE*, fopen/fclose
#include <stdlib.h>         // malloc, free, exit
#include <string.h>         // memset, memcpy, strlen, strcmp
#include <ctype.h>          // tolower, isdigit
#include <errno.h>          // errno, strerror

//directorios, archivos y procesos
#include <unistd.h>         // close, read, write, unlink, ftruncate
#include <sys/stat.h>       // mkdir, stat
#include <fcntl.h>          // open, flags como O_CREAT, O_RDWR
#include <sys/mman.h>       // mmap, munmap, msync
#include <dirent.h>         // DIR, struct dirent, opendir, readdir, closedir

//hilos
#include <pthread.h>        // pthread_create, pthread_join, mutex

//sockets
#include <sys/types.h>      // ssize_t, pid_t, etc.
#include <sys/socket.h>     // socket, bind, listen, accept, connect, send, recv
#include <netdb.h>          // getaddrinfo, getnameinfo
#include <arpa/inet.h>      // inet_ntoa, inet_pton (conversiones IP)
#include <netinet/in.h>     // sockaddr_in, htons, htonl

//hash
#include <openssl/md5.h>    // calculo de hash md5

//commons
#include <commons/log.h>           // log_create, log_info, log_error
#include <commons/config.h>        //  .config
#include <commons/string.h>        // F
#include <commons/bitarray.h>      // bitmap
#include <commons/collections/list.h> // listas

//control c
#include <signal.h>
#include <stdatomic.h>

extern t_log* logger;
extern t_config* config;

typedef enum {
    OP_CREATE_FILE ,
    OP_TRUNCATE_FILE,
    OP_TAG_FILE,
    OP_COMMIT_TAG,
    OP_WRITE_BLOCK,
    OP_READ_BLOCK,
    OP_DELETE_TAG,
    OP_FINISH,
    DESCONEXION
} t_storage_op;

typedef struct
{
	int size;
	void* stream;
} t_buffer;

typedef enum
{
	MENSAJE,
	PAQUETE
}op_code;
typedef struct
{
	op_code codigo_operacion;
	t_buffer* buffer;
} t_paquete;


int iniciar_servidor(char* puerto);
void* recibir_buffer(int*, int);
char* recibir_id(int);
t_storage_op string_a_operacion(const char* op_str);
t_storage_op recibir_operacion(int);
t_list* recibir_paquete(int socket_cliente);
t_config* iniciar_config(char* ruta_config);
t_log* iniciar_logger();
void enviar_paquete(t_paquete* ,int );
void eliminar_paquete(t_paquete*);
t_paquete* crear_paquete(void);
void agregar_a_paquete(t_paquete* , void* , int );
void crear_buffer(t_paquete* paquete);
#endif /* UTILS_H_ */
