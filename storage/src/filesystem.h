#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include "utils.h"

extern int block_size;
extern int fs_size;
extern int total_blocks;
extern char* punto_montaje;

void crear_path(char* dest, size_t dest_sz, const char* base, const char* leaf);
void crear_directorio(const char* path);
int eliminar_directorio_completo(const char* ruta);
void limpiar_contenido_directorio(const char* ruta_raiz);
void inicializar_fs();
void crear_superblock_config(const char* path_superblock);
void crear_bitmap_bin(char* path_bitmap);
void crear_blocks_hash_index_config(char* path_blocks_hash_index);
void crear_physical_blocks(char* path_physical_blocks);
void crear_initial_file(char* path_files, char* path_physical_blocks);

#endif /* FILESYSTEM_H_ */