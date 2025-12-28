#ifndef EXTRAS_OP_H_
#define EXTRAS_OP_H_

#include "utils.h"

extern pthread_mutex_t mutex_bitmap;
extern t_bitarray* bitmap;
extern int fs_size;
extern int block_size;
extern int total_blocks;
extern char* punto_montaje;

char* path_metadata(const char* file, const char* tag);
char* path_logical_block(const char* file, const char* tag, int nro_bloque);
char* path_block_fisico(int nro);

char* get_blocks_index_path(void);
char* get_bitmap_path(void);

int extraer_numero_bloque_fisico_de_path(const char* path_fisico);

bool persistir_bitmap_a_disco() ;

int buscar_en_blocks_index_por_hash(const char* hash);
void agregar_hash_blocks_index(const char* hash, int nro_bloque);
void remover_hash_blocks_index_por_bloque(int nro_bloque);

char* leer_bloque_fisico_en_buffer(int nro_fisico, int block_size, size_t* out_len);

int reservar_bloque(int query_id);
void liberar_bloque(int nro);

char* build_blocks_string_from_logical(const char* file, const char* tag, int count);

char* crypto_md5(const char* data, size_t len);

int copiar_archivo_con_hardlinks(const char* src, const char* dst);
int copiar_directorio_tag(const char* src, const char* dst);

#endif /* EXTRAS_OP_H_ */