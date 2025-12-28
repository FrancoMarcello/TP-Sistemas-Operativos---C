#include "filesystem.h"

char path_superblock[4096];
char path_bitmap[4096];
char path_blocks_hash_index[4096];
char path_physical_blocks[4096];
char path_files[4096];


void crear_path(char* dest, size_t dest_sz, const char* base, const char* leaf) {
    if (!dest || dest_sz == 0 || !base || !leaf) {
        if (dest_sz > 0) dest[0] = '\0';
        return;
    }

    size_t len = strlen(base);

    if(len > 0 && base[len - 1] == '/')
        snprintf(dest, dest_sz, "%s%s", base, leaf);
    else
        snprintf(dest, dest_sz, "%s/%s", base, leaf);
}

void crear_directorio(const char* path) {
    if (!path) return;

    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);

    size_t len = strlen(tmp);
    if (len == 0) return;

    if (tmp[len - 1] == '/')
        tmp[len - 1] = '\0';

    for(char *p = tmp + 1; *p; p++) {
        if(*p == '/') {
            *p = 0;
            mkdir(tmp, 0777);
            *p = '/';
        }
    }
    mkdir(tmp, 0777);
}

int eliminar_directorio_completo(const char* ruta) {
    DIR* directorio = opendir(ruta);
    if (!directorio) return -1;

    struct dirent* entrada;
    int resultado = 0;
    
    while ((entrada = readdir(directorio)) != NULL) {
        // ignorar "." y ".."
        if (!strcmp(entrada->d_name, ".") || !strcmp(entrada->d_name, ".."))
            continue;

        // crear ruta cmpleta
        size_t largo_total = strlen(ruta) + strlen(entrada->d_name) + 2;
        char* ruta_item = malloc(largo_total);

        if (!ruta_item) {
            resultado = -1;
            break;
        }

        snprintf(ruta_item, largo_total, "%s/%s", ruta, entrada->d_name);

        struct stat info;

        if (stat(ruta_item, &info) == 0) {
            if (S_ISDIR(info.st_mode)) {
                // si es directorio -> borrar recursivamente
                resultado = eliminar_directorio_completo(ruta_item);
            } else {
                // si es archivo -> borrar archivo
                resultado = unlink(ruta_item);
            }
        }

        free(ruta_item);
    }

    closedir(directorio);

    // borrar directorio vacio
    if (rmdir(ruta) != 0) return -1;

    return resultado;
}

void limpiar_contenido_directorio(const char* ruta_raiz) {
    if (!ruta_raiz) return;

    DIR* directorio = opendir(ruta_raiz);
    if (!directorio) return;

    struct dirent* entrada;

    while ((entrada = readdir(directorio)) != NULL) {
        // ignorar "." y ".."
        if (!strcmp(entrada->d_name, ".") || !strcmp(entrada->d_name, ".."))
            continue;

        char ruta_item[1024];
        snprintf(ruta_item, sizeof(ruta_item), "%s/%s", ruta_raiz, entrada->d_name);

        struct stat info;
        if (stat(ruta_item, &info) == 0) {
            if (S_ISDIR(info.st_mode)) {
                // borrar directorio completo
                eliminar_directorio_completo(ruta_item);
            } else {
                // borrar archivo
                unlink(ruta_item);
            }
        }
    }

    closedir(directorio);
}

void crear_superblock_config(const char* path_superblock){
    FILE *f = fopen(path_superblock, "w");
        if(!f){
        log_error(logger, "No se pudo crear el archivo superblock");
        exit(EXIT_FAILURE);
    }
    fprintf(f, "FS_SIZE=%d\nBLOCK_SIZE=%d\n", fs_size, block_size);
    fclose(f);
}

void crear_bitmap_bin(char* path_bitmap){
    size_t bytes = (total_blocks + 7) / 8; 
    
    //cracion
    int fd = open(path_bitmap, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        log_error(logger, "No se pudo crear el archivo bitmap");
        exit(EXIT_FAILURE);
    }

   //ajustar el tamaño
    if (ftruncate(fd, bytes) == -1) {
        log_error(logger, "No se pudo ajustar el tamaño del archivo bitmap");
        close(fd);
        exit(EXIT_FAILURE);
    }

    //mapear archivo en memoria
    void* buffer = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (buffer == MAP_FAILED) {
        log_error(logger, "No se pudo mapear el archivo bitmap a memoria");
        close(fd);
        exit(EXIT_FAILURE);
    }
    
    memset(buffer, 0, bytes);        // todos libres
    msync(buffer, bytes, MS_SYNC);   
    munmap(buffer, bytes);

    close(fd);
}

void crear_blocks_hash_index_config(char* path_blocks_hash_index){
    FILE* f = fopen(path_blocks_hash_index, "w");
    if(!f){
        log_error(logger, "No se pudo crear el archivo hash index");
        exit(EXIT_FAILURE);
    }
    fclose(f);
}

void crear_physical_blocks(char* path_physical_blocks){
    crear_directorio(path_physical_blocks);

    char block_path[512];
    char* buffer = malloc(block_size);

    if (!buffer) {
        log_error(logger, "No se pudo reservar buffer para physical blocks");
        exit(EXIT_FAILURE);
    }

    memset(buffer, 0, block_size);

    for (int i = 0; i < total_blocks; i++) {
        snprintf(block_path, sizeof(block_path), "%s/block%04d.dat", path_physical_blocks, i);
        FILE* block = fopen(block_path, "wb");
        if(!block){
            log_error(logger, "No se pudo crear el archivo physical blocks");
            free(buffer);
            exit(EXIT_FAILURE);
        }
        fwrite(buffer, 1, block_size, block);
        fclose(block);
    }
    free(buffer);
}

void crear_initial_file(char* path_files, char* path_physical_blocks) {
    char file_path[512], tag_path[512], metadata_path[512], logical_blocks_path[512], block0_path[512], hardlink_path[512];

    // ./files/initial_file
    crear_path(file_path, sizeof(file_path), path_files, "initial_file");
    crear_directorio(file_path);

    // ./files/initial_file/BASE
    crear_path(tag_path, sizeof(tag_path), file_path, "BASE");
    crear_directorio(tag_path);

    // ./files/initial_file/BASE/metadata.config
    crear_path(metadata_path, sizeof(metadata_path), tag_path, "metadata.config");
    
    FILE* metadata = fopen(metadata_path, "w");
    if (!metadata) {
        log_error(logger, "No se pudo crear el archivo metadata");
        exit(EXIT_FAILURE);
    }
    fprintf(metadata, "TAMAÑO=%d\n", block_size);   
    fprintf(metadata, "ESTADO=WORK_IN_PROGRESS\n");
    fprintf(metadata, "BLOCKS=[0]\n");
    fclose(metadata);

    // ./files/initial_file/BASE/logical_blocks
    crear_path(logical_blocks_path, sizeof(logical_blocks_path), tag_path, "logical_blocks");
    crear_directorio(logical_blocks_path);

    // hard link del bloque lógico 000000.dat al block0000.dat
    crear_path(block0_path, sizeof(block0_path), path_physical_blocks, "block0000.dat");
    crear_path(hardlink_path, sizeof(hardlink_path), logical_blocks_path, "000000.dat");

    unlink(hardlink_path);  // borra si existia
    if (link(block0_path, hardlink_path) == -1) {
        log_error(logger, "link hardlink block0");
        exit(EXIT_FAILURE);
    }

    // sñadir hash del bloque 0 a blocks_hash_index.config
    char blocks_hash_index[512];
    crear_path(blocks_hash_index, sizeof(blocks_hash_index), path_physical_blocks, "../blocks_hash_index.config");
    
    // block_path para md5
    FILE* f = fopen(block0_path, "rb");
    if (!f) return;
    
    unsigned char *buf = malloc(block_size);
    if (!buf) {
        fclose(f);
        return;
    }

    size_t leidos = fread(buf, 1, block_size, f);
     if (leidos < block_size)
        memset(buf + leidos, 0, block_size - leidos);
    fclose(f);
        
    // calcular MD5
     unsigned char md5sum[MD5_DIGEST_LENGTH];
    MD5(buf, block_size, md5sum);
    free(buf);

    char hex[33]; 
    for (int i = 0; i < 16; i++) 
        sprintf(hex + i*2, "%02x", md5sum[i]);
    hex[32] = '\0';

     // escribir en blocks_hash_index.config
    FILE* idx = fopen(blocks_hash_index, "a");
    if (idx) {
        fprintf(idx, "%s=block%04d\n", hex, 0);
        fclose(idx);
    }
}

void inicializar_fs() {
    crear_path(path_superblock, sizeof(path_superblock), punto_montaje, "superblock.config");
    crear_path(path_bitmap, sizeof(path_bitmap), punto_montaje, "bitmap.bin");
    crear_path(path_blocks_hash_index, sizeof(path_blocks_hash_index), punto_montaje, "blocks_hash_index.config");
    crear_path(path_physical_blocks, sizeof(path_physical_blocks), punto_montaje, "physical_blocks");
    crear_path(path_files, sizeof(path_files), punto_montaje, "files");

    crear_directorio(punto_montaje);

    char* fresh_start = config_get_string_value(config, "FRESH_START");

    if (strcasecmp(fresh_start, "TRUE") == 0) {
        limpiar_contenido_directorio(punto_montaje);
        
        crear_superblock_config(path_superblock);
        crear_bitmap_bin(path_bitmap);
        crear_blocks_hash_index_config(path_blocks_hash_index);
    
        crear_physical_blocks(path_physical_blocks);
        crear_directorio(path_files);

        crear_initial_file(path_files, path_physical_blocks);

        log_info(logger, "FS inicializado con exito en %s", punto_montaje);
    } else {
        log_info(logger, "Cargando FS existente en %s", punto_montaje);
    }
}
