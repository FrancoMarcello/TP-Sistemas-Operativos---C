#include<sys/socket.h>
#include<netdb.h>
#include<string.h>
#include<unistd.h>
#include<commons/log.h>
#include<commons/config.h>
#include <time.h>
#include <limits.h>
#include <sys/time.h>
typedef enum {
    OP_CREATE_FILE,
    OP_TRUNCATE_FILE,
    OP_TAG_FILE,
    OP_COMMIT_TAG,
    OP_WRITE_BLOCK,
    OP_READ_BLOCK,
    OP_DELETE_TAG,
    OP_FINISH,
    DESCONEXION
} t_storage_op;

typedef enum {
    LECTURA,
    FIN,
    DESALOJO,
    DESCONEXION_WORKER
} t_master_op;

typedef enum {
    WORKER_LIBRE,
    DESALOJO_QUERY,
    DESALOJO_PASIVO
} t_worker_op;

typedef struct{
    int numero_pagina;
    int marco;
    bool bit_presencia;
    bool bit_uso;
    bool bit_modificado;
    long ultima_referencia;
}t_pagina;

typedef struct{
    char* file;
    char* tag;
    t_list* paginas;
}t_tabla_paginas;

typedef struct{
    int marco;
    bool libre;
}t_marco;

void enviar_id(int conexion, int id_worker);
void* query_interpreter(void*);
t_instruction castear_instruccion(char* instruccion);
//intrucciones
void ejecutar_create(int id, char* file, char* tag);
void ejecutar_truncate(int id, char* file, char* tag, int tamanio);
void ejecutar_write(int id, char* file, char* tag, int dir_base, char* contenido);
void ejecutar_read(int id, char* file, char* tag, int dir_base, int tamanio);
void ejecutar_tag(int id, char* file_origen, char* tag_origen, char* file_destino, char* tag_destino);
void ejecutar_commit(int id, char* file, char* tag);
void ejecutar_flush(int id, char* file, char* tag);
void ejecutar_delete(int id, char* file, char* tag);
void ejecutar_end(query_worker_h* query_a_finalizar);
void ejecutar_unknow();
void enviar_operacion(int socket, t_storage_op operacion);
t_tabla_paginas* obtener_tabla(char* file,char* tag);
t_pagina* obtener_pagina(t_tabla_paginas* tabla, int pagina_buscar);
t_marco * asignar_marco();
t_pagina* algoritmo_lru();
void liberar_marco(int numero_marco);
void liberar_tabla_completa(t_tabla_paginas* tabla);
void flush_a_storage(t_pagina* pagina_victima,int id,  char* file, char* tag);
t_tabla_paginas* obtener_tabla_por_pagina(t_pagina* pagina_victima);
void cargar_pagina_desde_storage(t_tabla_paginas* tabla, t_pagina* pagina,int id,  char* file, char* tag, int numero_pagina);
t_pagina * algoritmo_clock();
bool comparar_paginas_por_marco(t_pagina* a, t_pagina* b);
long obtener_timestamp_ms();