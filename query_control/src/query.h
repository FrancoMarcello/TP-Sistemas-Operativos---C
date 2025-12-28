#ifndef QUERY_H_
#define QUERY_H_

#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <readline/readline.h>

//#include "utils.h"
#include <utils/utils.h>

//t_config* iniciar_config(char* archivo_config);
//t_log* iniciar_logger(t_config* config);
void enviar_query_a_master(int conexion_master,int id, char* argv[]);
void terminar_query_abruptamente(int);
void terminar_programa(int, t_log*, t_config*);
int handshake_query(int conexion);

typedef enum {
    LECTURA,
    FIN,
    DESALOJO,
    DESCONEXION_WORKER,
    DESCONEXION_QUERY,
    EJECUTAR_QUERY
} t_master_op;

#endif /* QUERY_H_ */
