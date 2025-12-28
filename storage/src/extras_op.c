#include "extras_op.h"

char* path_metadata(const char* file, const char* tag) {
    char* punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");
    return string_from_format("%s/files/%s/%s/metadata.config", punto_montaje, file, tag);
}

char* path_logical_block(const char* file, const char* tag, int nro_bloque) {
    char* punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");
    return string_from_format("%s/files/%s/%s/logical_blocks/%06d.dat", punto_montaje, file, tag, nro_bloque);
}

char* path_block_fisico(int nro) {
    char* punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");
    return string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje, nro);
}

char* get_blocks_index_path(void) {
    char* punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");
    char* p = string_from_format("%s/blocks_hash_index.config", punto_montaje);
    return p;
}

char* get_bitmap_path(void) {
    char* punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");
    char* p = string_from_format("%s/bitmap.bin", punto_montaje);
    return p;
}

int extraer_numero_bloque_fisico_de_path(const char* path_fisico) {
    const char* base = strrchr(path_fisico, '/');
    if (!base) base = path_fisico;
    else base++; // skip '/'
    
    int nro = -1;
    if (sscanf(base, "block%d.dat", &nro) == 1) return nro;
    
    return -1;
}

bool persistir_bitmap_a_disco() {
    char* bmp_path = get_bitmap_path();
    
    FILE* f = fopen(bmp_path, "wb");
    if (!f) {
        log_error(logger, "No se pudo persistir bitmap en %s: %s", bmp_path, strerror(errno));
        free(bmp_path);
        return false;
    }
    // bitmap->size es tamaño en bytes
    fwrite(bitmap->bitarray, 1, bitmap->size, f);
    fclose(f);
    free(bmp_path);
    return true;
}

int buscar_en_blocks_index_por_hash(const char* hash) {
    char* index_path = get_blocks_index_path();
    FILE* f = fopen(index_path, "r");
    if (!f) { 
        free(index_path); 
        return -1; 
    }
    
    char* linea = NULL;
    size_t len = 0;
    int encontrado = -1;
    
    while (getline(&linea, &len, f) != -1) {
        // formato: <md5>=block0000
        char* eq = strchr(linea, '=');
        if (!eq) continue;
        
        *eq = '\0';
        char* md5 = linea;
        char* blockname = eq + 1;
        // quitar newline
        char* nl = strchr(blockname, '\n');
        if (nl) *nl = '\0';
        
        if (strcmp(md5, hash) == 0) {
            // parsear nro de block
            int nro = -1;
            if (sscanf(blockname, "block%04d", &nro) == 1) {
                encontrado = nro;
                break;
            }
        }
    }
    free(linea);
    fclose(f);
    free(index_path);
    return encontrado;
}

void agregar_hash_blocks_index(const char* hash, int nro_bloque) {
    int existente = buscar_en_blocks_index_por_hash(hash);
    if (existente != -1) return;

    char* index_path = get_blocks_index_path();
    FILE* f = fopen(index_path, "a");
    
    if (!f) {
        log_error(logger, "No se pudo abrir blocks_hash_index.config para append: %s", strerror(errno));
        free(index_path);
        return;
    }
    
    fprintf(f, "%s=block%04d\n", hash, nro_bloque);
    fclose(f);
    free(index_path);
}

void remover_hash_blocks_index_por_bloque(int nro_bloque) {
    char* index_path = get_blocks_index_path();
    char* temp_path = string_from_format("%s.tmp", index_path);
    
    FILE* fin = fopen(index_path, "r");
    FILE* fout = fopen(temp_path, "w");
    
    if (!fin || !fout) {
        log_error(logger, "Error manipulando blocks_hash_index.config: %s", strerror(errno));
        if (fin) fclose(fin);
        if (fout) fclose(fout);
        unlink(temp_path);
        free(index_path);
        free(temp_path);
        return;
    }
    
    char* linea = NULL;
    size_t len = 0;
    
    while (getline(&linea, &len, fin) != -1) {
        // ver si la linea apunta al bloque nro_bloque
        char* eq = strchr(linea, '=');
        if (!eq) continue;
        
        char* blockname = eq + 1;
        char* nl = strchr(blockname, '\n');
        if (nl) *nl = '\0';
        
        int nro = -1;
        if (sscanf(blockname, "block%04d", &nro) == 1) {
            if (nro == nro_bloque) {
                // omitir línea -> remover
                continue;
            }
        }
        fprintf(fout, "%s\n", linea);
    }
    free(linea);
    fclose(fin);
    fclose(fout);
    // reemplazar archivo
    rename(temp_path, index_path);
    free(index_path);
    free(temp_path);
}

char* leer_bloque_fisico_en_buffer(int nro_fisico, int block_size, size_t* out_len) {
    char* path = path_block_fisico(nro_fisico);
    FILE* f = fopen(path, "rb");
    free(path);
    
    if (!f) return NULL;
    
    char* buf = malloc(block_size);
    if (!buf) { 
        fclose(f); 
        return NULL; 
    }
    
    size_t r = fread(buf, 1, block_size, f);
    fclose(f);
    
    if (out_len) *out_len = r;
    return buf;
}

int reservar_bloque(int query_id) {
    pthread_mutex_lock(&mutex_bitmap);

    for (int i = 0; i < total_blocks; i++) {
        if (!bitarray_test_bit(bitmap, i)) {
            bitarray_set_bit(bitmap, i);

            if (!persistir_bitmap_a_disco()) {
                // revertir
                bitarray_clean_bit(bitmap, i);
                pthread_mutex_unlock(&mutex_bitmap);
                return -1;
            }

            pthread_mutex_unlock(&mutex_bitmap);
            log_info(logger, "%d - Bloque Físico Reservado - Número de Bloque: %d", query_id, i);
            return i;
        }
    }

    pthread_mutex_unlock(&mutex_bitmap);
    return -1; // no hay lugar
}

void liberar_bloque(int nro) {
    pthread_mutex_lock(&mutex_bitmap);

    bitarray_clean_bit(bitmap, nro);

    persistir_bitmap_a_disco();

    pthread_mutex_unlock(&mutex_bitmap);

    log_info(logger, "Bloque Físico Liberado - Número de Bloque: %d", nro);
}

#include <sys/stat.h>

char* build_blocks_string_from_logical(const char* file, const char* tag, int count) {
    char* resultado = string_new();
    string_append(&resultado, "[");

    for (int i = 0; i < count; i++) {
        char* path_logico = path_logical_block(file, tag, i);
        struct stat stat_logico;

        // 1. Obtener el inodo del bloque lógico
        if (stat(path_logico, &stat_logico) != 0) {
            free(path_logico);
            free(resultado);
            return string_duplicate("[]");
        }
        free(path_logico);

        int bloque_fisico_encontrado = -1;

        // 2. Buscar qué bloque físico comparte el mismo inodo
        for (int j = 0; j < total_blocks; j++) {
            char* path_fisico = path_block_fisico(j);
            struct stat stat_fisico;
            
            if (stat(path_fisico, &stat_fisico) == 0) {
                if (stat_logico.st_ino == stat_fisico.st_ino) {
                    bloque_fisico_encontrado = j;
                    free(path_fisico);
                    break;
                }
            }
            free(path_fisico);
        }

        // 3. Validar si lo encontramos
        if (bloque_fisico_encontrado < 0) {
            free(resultado);
            return string_duplicate("[]");
        }

        // 4. Formatear el string
        if (i > 0) {
            string_append(&resultado, ",");
        }
        char* nro_str = string_itoa(bloque_fisico_encontrado);
        string_append(&resultado, nro_str);
        free(nro_str);
    }

    string_append(&resultado, "]");
    return resultado;
}


/*char* build_blocks_string_from_logical(const char* file, const char* tag, int count) {
    char* resultado = string_new();
    string_append(&resultado, "[");

    bool first = true;

    for (int i = 0; i < count; i++) {
        char* path_logico = path_logical_block(file, tag, i);

        if (!path_logico) {
            free(resultado);
            return string_duplicate("[]");
        }

        char realbuf[512];
        char* path_fisico  = realpath(path_logico, realbuf);
        free(path_logico);

        if (!path_fisico) {
            free(resultado);
            return string_duplicate("[]");
        }

        int nro = extraer_numero_bloque_fisico_de_path(path_fisico);

        if (nro < 0) {
            free(resultado);
            return string_duplicate("[]");
        }

        if (!first) string_append(&resultado, ",");
        first = false;

        char* nro_str = string_itoa(nro);
        string_append(&resultado, nro_str);
        free(nro_str);
    }

    string_append(&resultado, "]");
    return resultado;
}*/

int copiar_archivo_con_hardlinks(const char* src, const char* dst) {
    //nuevo
     if (strstr(src, "metadata.config") != NULL) {
        FILE* fsrc = fopen(src, "rb");
        if (!fsrc) return -1;
        FILE* fdst = fopen(dst, "wb");
        if (!fdst) { fclose(fsrc); return -1; }

        char buf[4096];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), fsrc)) > 0) {
            fwrite(buf, 1, n, fdst);
        }
        fclose(fsrc);
        fclose(fdst);
        return 0;
    }    
    
    // Intentar hardlink directo
    if (link(src, dst) == 0) return 0;

    // Si falla el hardlink copiar contenido real
    FILE* fsrc = fopen(src, "rb");
    if (!fsrc) return -1;

    FILE* fdst = fopen(dst, "wb");
    if (!fdst) {
        fclose(fsrc);
        return -1;
    }

    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fsrc)) > 0) {
        fwrite(buf, 1, n, fdst);
    }

    fclose(fsrc);
    fclose(fdst);
    return 0;
}

int copiar_directorio_tag(const char* src, const char* dst) {

    mkdir(dst, 0777);

    // Copiar metadata
    char src_meta[512], dst_meta[512];
    snprintf(src_meta, sizeof(src_meta), "%s/metadata.config", src);
    snprintf(dst_meta, sizeof(dst_meta), "%s/metadata.config", dst);

    if (copiar_archivo_con_hardlinks(src_meta, dst_meta) != 0)
        return -1;

    // Copiar logical_blocks/
    char src_lb[512], dst_lb[512];
    snprintf(src_lb, sizeof(src_lb), "%s/logical_blocks", src);
    snprintf(dst_lb, sizeof(dst_lb), "%s/logical_blocks", dst);

    mkdir(dst_lb, 0777);

    DIR* d = opendir(src_lb);
    if (!d) return -1;

    struct dirent* e;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;

        char* src_file = NULL;
        char* dst_file = NULL;
        
        if (asprintf(&src_file, "%s/%s", src_lb, e->d_name) < 0) { 
            closedir(d); 
            return -1; 
        }
        if (asprintf(&dst_file, "%s/%s", dst_lb, e->d_name) < 0) { 
            free(src_file); 
            closedir(d); 
            return -1; 
        }

        if (copiar_archivo_con_hardlinks(src_file, dst_file) != 0) {
            free(src_file);
            free(dst_file);
            closedir(d);
            return -1;
        }
        
        free(src_file);
        free(dst_file);

    }

    closedir(d);
    return 0;
}
