#include "operaciones.h"
#include "filesystem.h"

void crear_file(char* file, char* tag, int query_id){ 
    char* punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");

    char path_files[512];
    char path_file[512];
    char path_tag[512];
    char path_metadata[512];
    char path_logical_blocks[512];

    //crear directorio file
    crear_path(path_files, sizeof(path_files), punto_montaje, "files");
    mkdir(path_files, 0777); // tdo el mundo rwx
    
    crear_path(path_file, sizeof(path_file), path_files, file);
    mkdir(path_file, 0777);
    
    // crear directorio tag
    crear_path(path_tag, sizeof(path_tag), path_file, tag);
    
    // validar existencia previa: si ya existe -> error
    struct stat st;
    if (stat(path_tag, &st) == 0) {
        log_error(logger, "%d - File/Tag preexistente %s:%s", query_id, file, tag);
        return;
    }

    if (mkdir(path_tag, 0777) != 0) {
        log_error(logger, "%d - No se pudo crear directorio tag %s: %s", query_id, path_tag, strerror(errno));
        return;
    }
    
    // crear directorio logical_blocks
    crear_path(path_logical_blocks, sizeof(path_logical_blocks), path_tag, "logical_blocks");
    mkdir(path_logical_blocks, 0777);
    
    // crear metadata.config
    crear_path(path_metadata, sizeof(path_metadata), path_tag, "metadata.config");

    FILE* f = fopen(path_metadata, "w");
    if (!f) {
        log_error(logger, "No se pudo crear el archivo de metadata en %s", path_metadata);
        return;
    }

    fprintf(f, "TAMAÑO=0\n");
    fprintf(f, "ESTADO=WORK_IN_PROGRESS\n");
    fprintf(f, "BLOCKS=[]\n");
    fclose(f);

    log_info(logger, "%d - File Creado %s:%s", query_id, file, tag);
}

void truncar_file(const char* file, const char* tag, int new_size, int query_id) {
    char* path = path_metadata(file, tag);

    t_config* metadata = config_create(path);
    if (!metadata) {
        log_error(logger, "%d - Tag inexistente %s:%s", query_id, file, tag);
        free(path);
        return;
    }

    if (new_size < 0) {
        log_error(logger, "%d - Tamaño negativo inválido %d para %s:%s", query_id, new_size, file, tag);
        config_destroy(metadata);
        free(path);
        return;
    }

    // no truncar si esta commited
    if (strcmp(config_get_string_value(metadata, "ESTADO"), "COMMITED") == 0) {
        log_error(logger, "%d - Truncado no permitido en %s:%s (estado COMMITED)", query_id, file, tag);
        config_destroy(metadata);
        free(path);
        return;
    }

    int current_size_bytes = config_get_int_value(metadata, "TAMAÑO");
    int total_blocks_actual = (current_size_bytes + block_size - 1 )/ block_size;
    int bloques_nuevos = (new_size + block_size - 1) / block_size;

    if (bloques_nuevos > total_blocks_actual) {// agrandar
        for (int i = total_blocks_actual; i < bloques_nuevos; i++) {
            char* path_logico = path_logical_block(file, tag, i);
            char* path_fisico = path_block_fisico(0);

            if (link(path_fisico, path_logico) != 0) {
                log_error(logger, "%d - No se pudo linkear bloque lógico %d a block0: %s", query_id, i, strerror(errno));
            } else {
                log_info(logger, "%d - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico 0", query_id, file, tag, i);
            }

            free(path_logico);
            free(path_fisico);
        }
    }else if (bloques_nuevos < total_blocks_actual)    //achicar
    {
        for (int i = total_blocks_actual -1; i >= bloques_nuevos; i--) {
            char* path_logico = path_logical_block(file, tag, i);

            // obtener bloque fisico al que apunta
            char realbuf[512];
            char* path_fisico = realpath(path_logico, realbuf);

            if (path_fisico) {
                int nro_fisico = extraer_numero_bloque_fisico_de_path(path_fisico);

                // eliminar el hard link
                if (unlink(path_logico) == 0) {
                    log_info(logger, "%d - %s:%s Se eliminó el hard link del bloque lógico %d al bloque físico %d", query_id, file, tag, i, nro_fisico);
                } else {
                    log_error(logger, "%d - Error al eliminar hard link %s: %s", query_id, path_logico, strerror(errno));
                }

                //revisar q el bloque no tenga refes
                struct stat st;
                if (stat(path_fisico, &st) == 0) {
                    if (st.st_nlink == 1) {
                        liberar_bloque(nro_fisico);
                        remover_hash_blocks_index_por_bloque(nro_fisico);
                    }
                }
            } else {
                // realpath falló; intentar unlink
                unlink(path_logico);
                log_warning(logger, "%d - No se pudo obtener realpath de %s", query_id, path_logico);
            }
    
            free(path_logico);
        }
    }

    // actualizar metadata: TAMAÑO y BLOCKS
    char* size_str = string_itoa(new_size);
    config_set_value(metadata, "TAMAÑO", size_str);
    free(size_str);

    // reconstruir BLOCKS
    char* blocks_str = build_blocks_string_from_logical(file, tag, bloques_nuevos);
    if (blocks_str) {
        config_set_value(metadata, "BLOCKS", blocks_str);
        free(blocks_str);
    }

    config_save(metadata);
    config_destroy(metadata);
    free(path);

    log_info(logger, "%d - File Truncado %s:%s - Tamaño: %d", query_id, file, tag, new_size);
}


void write_block(const char* file, const char* tag, int nro_bloque, const char* contenido, int query_id) {
    char* meta_path = path_metadata(file, tag);
    if (!meta_path) return;
    
    t_config* meta = config_create(meta_path);
    if (!meta) {
        log_error(logger, "%d - Tag inexistente %s:%s", query_id, file, tag);
        free(meta_path);
        return;
    }
    
    // validar COMMITED
    const char* estado = config_get_string_value(meta, "ESTADO");
    if (estado && strcmp(estado, "COMMITED") == 0) {
        log_error(logger, "%d - Escritura no permitida en %s:%s (estado COMMITED)", query_id, file, tag);
        config_destroy(meta);
        free(meta_path);
        return;
    }

    // validar que el bloque lógico exista dentro del tamaño actual
    int tamanio = config_get_int_value(meta, "TAMAÑO");

    config_destroy(meta);
    free(meta_path);

    int bloques_asignados = (tamanio + block_size - 1) / block_size;
    
    if (nro_bloque >= bloques_asignados) {
        log_error(logger, "%d - Escritura fuera del tamaño actual en %s:%s - Bloque: %d", query_id, file, tag, nro_bloque);
        return;
    }

    char* path_logico = path_logical_block(file, tag, nro_bloque);
    struct stat st;

    if (stat(path_logico, &st) != 0) {
        log_error(logger, "%d - Error al abrir bloque lógico %s:%s - Número de Bloque: %d (no existe)", query_id, file, tag, nro_bloque);
        free(path_logico);
        return;
    }

    // obtener el path fisico al que apunta
    char realbuf[512];
    char* path_fisico_actual = realpath(path_logico, realbuf);
    if (!path_fisico_actual) {
        log_error(logger, "%d - No se pudo resolver bloque físico para %s:%s - %s", query_id, file, tag, strerror(errno));
        free(path_logico);
        return;
    }

    struct stat st_fisico;
    if (stat(path_fisico_actual, &st_fisico) != 0) {
        log_error(logger, "%d - No se pudo stat físico %s", query_id, path_fisico_actual);
        free(path_logico);
        return;
    }

    bool compartido = (st_fisico.st_nlink > 1);

    // Variables de escritura segura (fuera del if para mejor legibilidad, pero usadas dentro)
    size_t len_contenido = strlen(contenido);
    size_t bytes_contenido_real = (len_contenido > block_size) ? block_size : len_contenido;
    size_t rellenar = block_size - bytes_contenido_real;

    if (compartido) {
        // reservar nuevo bloque físico
        int nuevo_nro = reservar_bloque(query_id);
        if (nuevo_nro < 0) {
            log_error(logger, "%d - No hay espacio para reservar nuevo bloque físico", query_id);
            free(path_logico);
            return;
        }

        char* path_fisico_nuevo = path_block_fisico(nuevo_nro);

        // escribir contenido en bloque físico nuevo 
        FILE* fw = fopen(path_fisico_nuevo, "wb");
        if (!fw) {
            log_error(logger, "%d - No se pudo abrir block físico nuevo para escribir: %s", query_id, strerror(errno));
            liberar_bloque(nuevo_nro);
            free(path_fisico_nuevo);
            free(path_logico);
            return;
        }
        
        // 🛠️ INICIO CORRECCIÓN COW (Escritura Segura) 🛠️
        fwrite(contenido, 1, bytes_contenido_real, fw);

        if (rellenar > 0) {
            char* ceros = calloc(1, rellenar);
            if (ceros) {
                fwrite(ceros, 1, rellenar, fw);
                free(ceros);
            }
        }
        // 🛠️ FIN CORRECCIÓN COW 🛠️

        fclose(fw);
        
        //Elimina hard link lógico existente.
        if(unlink(path_logico)!=0){
            log_error(logger, "%d - error al eliminar el hard link previo %s: %s", query_id,path_logico, strerror(errno));
            liberar_bloque(nuevo_nro);
            free(path_logico);
            return;
        }

        //linkear nuevo
        if (link(path_fisico_nuevo, path_logico) != 0) {
            log_error(logger, "%d - No se pudo linkear bloque lógico a nuevo físico: %s", query_id, strerror(errno));
            liberar_bloque(nuevo_nro);
            free(path_fisico_nuevo);
            free(path_logico);
            return;
        }

        log_info(logger, "%d - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico %d (COW)", query_id, file, tag, nro_bloque, nuevo_nro);

        free(path_fisico_nuevo);
    } else {
        // único link -> escribir directamente en el bloque físico
        
        FILE* fw = fopen(path_fisico_actual, "wb");
        if (!fw) {
            log_error(logger, "%d - Error al abrir bloque físico para escritura: %s", query_id, strerror(errno));
            free(path_logico);
            return;
        }

        // 🛠️ INICIO CORRECCIÓN DIRECTA (Escritura Segura) 🛠️
        fwrite(contenido, 1, bytes_contenido_real, fw);
        
        if (rellenar > 0) {
            char* ceros = calloc(1, rellenar);
            if (ceros) {
                fwrite(ceros, 1, rellenar, fw);
                free(ceros);
            }
        }
        // 🛠️ FIN CORRECCIÓN DIRECTA 🛠️

        fclose(fw);
    }

    free(path_logico);
    log_info(logger, "%d - Bloque Lógico Escrito %s:%s - Número de Bloque: %d", query_id, file, tag, nro_bloque);

}

char* read_block(const char* file, const char* tag, int nro_bloque, int query_id) {
    char* meta_path = path_metadata(file, tag);
    t_config* meta = config_create(meta_path);
    if (!meta) {
        log_error(logger, "%d - Tag inexistente %s:%s", query_id, file, tag);
        free(meta_path);
        return strdup("");
    }

    if (nro_bloque >= total_blocks) {
        log_error(logger, "%d - Lectura fuera de límite en %s:%s", query_id, file, tag);
        config_destroy(meta);
        free(meta_path);
        return strdup("");
    }

    // validar contra tamaño actual
    int size = config_get_int_value(meta, "TAMAÑO");

    int bloques_asignados = (size + block_size - 1) / block_size;

    if (nro_bloque >= bloques_asignados) {
        log_error(logger, "%d - Lectura fuera del tamaño actual en %s:%s - Bloque: %d", query_id, file, tag, nro_bloque);
        config_destroy(meta);
        free(meta_path);
        return strdup("");
    }

    config_destroy(meta);
    free(meta_path);
    
    char* path_logico = path_logical_block(file, tag, nro_bloque);

    FILE* f = fopen(path_logico, "r");
    if (!f) {
        free(path_logico);
        log_error(logger, "%d - Bloque inexistente %s:%s - Número de Bloque: %d", query_id, file, tag, nro_bloque);
        return strdup("");
    }

    //leer bloque
    char* buffer = malloc(block_size + 1);
    size_t leidos = fread(buffer, 1, block_size, f);
    buffer[leidos] = '\0';
    fclose(f);
    free(path_logico);

    log_info(logger, "%d - Bloque Lógico Leído %s:%s - Número de Bloque: %d", query_id, file, tag, nro_bloque);
    return buffer;
}

void tag_file(const char* file, const char* tag_src, const char* tag_dst, int query_id) {
        
    char src[512];
    char dst[512];

    snprintf(src, sizeof(src), "%s/files/%s/%s", punto_montaje, file, tag_src);
    snprintf(dst, sizeof(dst), "%s/files/%s/%s", punto_montaje, file, tag_dst);

    // validar existencia fuente
    struct stat st;
    if (stat(src, &st) != 0) {
        log_error(logger, "%d - Tag inexistente %s:%s", query_id, file, tag_src);
        return;
    }   

    // verificar destino no existe
    if (stat(dst, &st) == 0) {
        log_error(logger, "%d - Tag destino ya existe %s:%s", query_id, file, tag_dst);
        return;
    }

      // copiar estructura TAG completa
    if (copiar_directorio_tag(src, dst) != 0) {
        log_error(logger, "%d - Error copiando tag %s:%s", query_id, file, tag_dst);
        return;
    }

    // estado en WORK_IN_PROGRESS
    char dst_meta[512];
    int written = snprintf(dst_meta, sizeof(dst_meta), "%s/metadata.config", dst);
    if (written < 0 || written >= sizeof(dst_meta)) {
        log_error(logger, "Path demasiado largo en tag_file()");
        return;
    }

    t_config* meta = config_create(dst_meta);
    if (meta) {
        config_set_value(meta, "ESTADO", "WORK_IN_PROGRESS");
        config_save(meta);
        config_destroy(meta);
    }

    log_info(logger, "%d - Tag creado %s:%s", query_id, file, tag_dst);
}

void commit_tag(const char* file, const char* tag, int query_id) {
    char* path = path_metadata(file, tag);

    t_config* metadata = config_create(path);
    if (!metadata) {
        log_error(logger, "%d - Tag inexistente %s:%s", query_id, file, tag);
        free(path);
        return;
    }

    const char* estado = config_get_string_value(metadata, "ESTADO");
    if (estado && strcmp(estado, "COMMITED") == 0) {
        config_destroy(metadata);
        free(path);
        return;
    }

    int size = config_get_int_value(metadata, "TAMAÑO");
    int bloques = (size > 0) ? (size + block_size - 1) / block_size : 0;

    for (int i = 0; i < bloques; i++) {
        char* path_logico = path_logical_block(file, tag, i);
        struct stat st_logico;

        if (stat(path_logico, &st_logico) != 0) {
            free(path_logico);
            continue;
        }

        // --- 🛠️ CAMBIO CLAVE AQUÍ: Buscar bloque físico por inodo ---
        int nro_fisico_actual = -1;
        for (int j = 0; j < total_blocks; j++) {
            char* p_fisico = path_block_fisico(j);
            struct stat st_fisico;
            if (stat(p_fisico, &st_fisico) == 0) {
                if (st_logico.st_ino == st_fisico.st_ino) {
                    nro_fisico_actual = j;
                    free(p_fisico);
                    break;
                }
            }
            free(p_fisico);
        }
        // ----------------------------------------------------------

        if (nro_fisico_actual < 0) {
            log_warning(logger, "%d - No se pudo encontrar bloque físico para lógico %d", query_id, i);
            free(path_logico);
            continue;
        }

        size_t len = 0;
        char* buf = leer_bloque_fisico_en_buffer(nro_fisico_actual, block_size, &len);
        if (!buf) {
            free(path_logico);
            continue;
        }

        char* md5 = crypto_md5(buf, len);
        if (!md5) {
            free(buf);
            free(path_logico);
            continue;
        }

        int bloque_confirmado = buscar_en_blocks_index_por_hash(md5);

        if (bloque_confirmado >= 0) {
            if (bloque_confirmado != nro_fisico_actual) {
                char* path_fisico_confirmado = path_block_fisico(bloque_confirmado);
                unlink(path_logico);
                if (link(path_fisico_confirmado, path_logico) == 0) {
                    log_info(logger, "%d - %s:%s Reasignado de %d a %d", query_id, file, tag, i, nro_fisico_actual, bloque_confirmado);
                }

                char* path_actual = path_block_fisico(nro_fisico_actual);
                struct stat st_act;
                if (stat(path_actual, &st_act) == 0 && st_act.st_nlink <= 1) {
                    liberar_bloque(nro_fisico_actual);
                    remover_hash_blocks_index_por_bloque(nro_fisico_actual);
                }
                free(path_actual);
                free(path_fisico_confirmado);
            }
        } else {
            // Esto es lo que no te funcionaba antes porque el nro era -1
            agregar_hash_blocks_index(md5, nro_fisico_actual); 
            log_info(logger, "%d - Nuevo hash agregado: bloque %d", query_id, nro_fisico_actual);
        }

        free(md5);
        free(buf);
        free(path_logico);
    }

    // Actualizar metadata final
    char* blocks_str = build_blocks_string_from_logical(file, tag, bloques);
    FILE* f = fopen(path, "w");
    if (f) {
        fprintf(f, "TAMAÑO=%d\nBLOCKS=%s\nESTADO=COMMITED\n", size, blocks_str);
        fclose(f);
    }
    free(blocks_str);
    config_destroy(metadata);
    free(path);
}


/*void commit_tag(const char* file, const char* tag, int query_id) {
    char* path = path_metadata(file, tag);

    t_config* metadata = config_create(path);
    if (!metadata) {
        log_error(logger, "%d - Tag inexistente %s:%s", query_id, file, tag);
        free(path);
        return;
    }

    const char* estado = config_get_string_value(metadata, "ESTADO");
    if (estado && strcmp(estado, "COMMITED") == 0) {
        config_destroy(metadata);
        free(path);
        return;
    }

    // obtener tamaño y block_size
    int size = config_get_int_value(metadata, "TAMAÑO");
    //int block_size = config_get_int_value(config, "BLOCK_SIZE");

    //cant bloques
    int bloques = 0;
    if (size > 0) {
        bloques = (size + block_size - 1) / block_size;
    } else {
        bloques = 0;
    }

    // recorrer cada bloque lógico
    for (int i = 0; i < bloques; i++) {

        char* path_logico = path_logical_block(file, tag, i);

        // si no existe el hardlink lógico -> saltar
        struct stat st;
        if (stat(path_logico, &st) != 0) {
            free(path_logico);
            continue;
        }

        // resolver físico real
        char realbuf[512];
        char* path_fisico = realpath(path_logico, realbuf);
        if (!path_fisico) {
            log_warning(logger, "%d - No se pudo resolver realpath de %s", query_id, path_logico);
            free(path_logico);
            continue;
        }

        int nro_fisico_actual = extraer_numero_bloque_fisico_de_path(path_fisico);
        if (nro_fisico_actual < 0) {
            free(path_logico); // no hacemos nada
            continue;
        }

        // leer contenido del bloque físico
        size_t len = 0;
        char* buf = leer_bloque_fisico_en_buffer(nro_fisico_actual, block_size, &len);
        if (!buf) {
             log_warning(logger, "%d - No se pudo leer bloque físico %d (lógico %d) de %s", query_id, nro_fisico_actual, i, path_logico);
            free(path_logico);
            continue;
        }

        // calcular md5
        char* md5 = crypto_md5(buf, len);
         if (!md5) {
            free(buf);
            free(path_logico);
            continue;
        }

        // buscar si MD5 ya existe en index
        int bloque_confirmado = buscar_en_blocks_index_por_hash(md5);

        if (bloque_confirmado >= 0) {
            // existe un bloque con mismo contenido
            if (bloque_confirmado != nro_fisico_actual) {
                // reasignar el hardlink lógico al bloque confirmado
                char* path_fisico_confirmado = path_block_fisico(bloque_confirmado);
                if (!path_fisico_confirmado) {
                    log_error(logger, "%d - No se pudo obtener path del bloque confirmado %d", query_id, bloque_confirmado);
                } else {
                    // unlink del logico
                    if (unlink(path_logico) != 0) {
                        log_warning(logger, "%d - No se pudo unlinkear %s antes de linkear confirmado: %s", query_id, path_logico, strerror(errno));
                    }

                    if (link(path_fisico_confirmado, path_logico) != 0) {
                        log_error(logger, "%d - No se pudo linkear %s a block confirmado %s: %s", query_id, path_logico, path_fisico_confirmado, strerror(errno));
                    } else {
                        log_info(logger, "%d - %s:%s Bloque Lógico %d se reasigna de %d a %d", query_id, file, tag, i, nro_fisico_actual, bloque_confirmado);

                    }

                    // verificar si el bloque físico actual quedó sin referencias
                    char* path_actual = path_block_fisico(nro_fisico_actual);
                    if (path_actual) {
                        struct stat st_act;
                        if (stat(path_actual, &st_act) == 0) {
                            // st_nlink cuenta el archivo en physical_blocks + links lógicos; si == 1 -> nadie más referencia
                            if (st_act.st_nlink <= 1) {
                                liberar_bloque(nro_fisico_actual);
                                remover_hash_blocks_index_por_bloque(nro_fisico_actual);
                                log_info(logger, "%d - Bloque físico %d liberado (sin referencias)", query_id, nro_fisico_actual);
                            }
                        } else {
                            log_warning(logger, "%d - No se pudo statear %s para decidir liberación: %s", query_id, path_actual, strerror(errno));
                        }
                        free(path_actual);
                    }

                    free(path_fisico_confirmado);
                }
            } else {
                // el bloque confirmado es el mismo que el actual
                log_debug(logger, "%d - Bloque lógico %d ya apunta al bloque confirmado %d", query_id, i, nro_fisico_actual);
            }
        } else {
            // no existe -> agregar md5 al index apuntando al bloque actual
            agregar_hash_blocks_index(md5, nro_fisico_actual);
            log_info(logger, "%d - Nuevo hash agregado al index: bloque %d (lógico %d)", query_id, nro_fisico_actual, i);
        }

        free(md5);
        free(buf);
        free(path_logico);
    }

    int sizee = 0;
    if (metadata) {
        sizee = config_get_int_value(metadata, "TAMAÑO");
        config_destroy(metadata);
    }   

    // reconstruir BLOCKS 
    char* blocks_str = build_blocks_string_from_logical(file, tag, bloques);

    FILE* f = fopen(path, "w");
    if (!f) {
        log_warning(logger, "%d - No se pudo abrir metadata para escribir: %s", query_id, strerror(errno));
    } else { // sino tira error
        fprintf(f, "TAMAÑO=%d\n", sizee);
        fprintf(f, "BLOCKS=%s\n", blocks_str);
        fprintf(f, "ESTADO=COMMITED\n");
        fclose(f);
    }

    free(blocks_str);
    free(path);

    log_info(logger, "%d - Commit de File:Tag %s:%s completado (bloques: %d)", query_id, file, tag, bloques);
}*/

void delete_tag(const char* file, const char* tag, int query_id) {
    char* punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");
    char* path_tag = string_from_format("%s/files/%s/%s", punto_montaje, file, tag);

    // recorrer logical_blocks y liberar físicos si corresponde
    char* path_logical_blocks = string_from_format("%s/logical_blocks", path_tag);
    DIR* d = opendir(path_logical_blocks);

    if (d) {
        struct dirent* entry;
        while ((entry = readdir(d)) != NULL) {

            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;

            if (entry->d_type == DT_REG) {
                // armar path lógico
                char* path_logico = string_from_format("%s/%s", path_logical_blocks, entry->d_name);

                // obtener físico apuntado
                char realbuf[256];
                char* path_fisico = realpath(path_logico, realbuf);

                if (path_fisico) {
                    struct stat st;
                    if (stat(path_fisico, &st) == 0) {
                        int nro_fisico = extraer_numero_bloque_fisico_de_path(path_fisico);

                        bool liberar = (st.st_nlink == 1);

                        // eliminar hardlink lógico
                        unlink(path_logico);

                        if (liberar && nro_fisico >= 0) {
                            liberar_bloque(nro_fisico);
                            remover_hash_blocks_index_por_bloque(nro_fisico);
                        }
                    } else {
                        // no se pudo stat
                        unlink(path_logico);
                    }
                } else {
                    // realpath falló 
                    unlink(path_logico);
                }

                free(path_logico);
            }
        }
        closedir(d);
    }

    free(path_logical_blocks);

    eliminar_directorio_completo(path_tag);

    free(path_tag);

    log_info(logger, "%d - Tag Eliminado %s:%s", query_id, file, tag);
}
