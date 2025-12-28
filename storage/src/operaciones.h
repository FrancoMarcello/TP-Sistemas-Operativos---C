#ifndef OPERACIONES_H_
#define OPERACIONES_H_

#include "extras_op.h"
#include "operaciones.h"
#include "filesystem.h"

extern pthread_mutex_t mutex_bitmap;
extern t_bitarray* bitmap;
extern int fs_size;
extern int block_size;
extern int total_blocks;
extern char* punto_montaje;

void crear_file(char* file, char* tag, int query_id);
void truncar_file(const char* file, const char* tag, int new_size, int query_id);
void write_block(const char* file, const char* tag, int num_block, const char* data, int query_id);
char* read_block(const char* file, const char* tag, int num_block, int query_id);
void tag_file(const char* file, const char* tag_src, const char* tag_dst, int query_id);
void commit_tag(const char* file, const char* tag, int query_id);
void delete_tag(const char* file, const char* tag, int query_id);
#endif /* OPERACIONES_H_ */