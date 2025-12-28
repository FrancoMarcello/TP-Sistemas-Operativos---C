#include "query.h"

// ./bin/query_control "query.config" "query_prueba" "1"
int id_designado;
int conexion_master;

int main(int argc, char* argv[]) {

    signal(SIGINT, terminar_query_abruptamente); //Ctrl+C
    signal(SIGHUP, terminar_query_abruptamente); //Cierre de terminal.

    if (argc < 4)
    {
        printf("Faltan parámetros al iniciar query");
    }
    config = config_create(argv[1]);
    logger = log_create("master.log","MASTER",1,log_level_from_string(config_get_string_value(config,"LOG_LEVEL")));
//   t_config* config = iniciar_config(argv[1]);
//    t_log* logger = iniciar_logger(config);
    log_info(logger, "## Config y logger iniciados exitosamente");

    char* ip = config_get_string_value(config, "IP_MASTER");
    char* puerto = config_get_string_value(config, "PUERTO_MASTER");

    conexion_master = crear_conexion(ip, puerto);
    log_info(logger, "## Conexión al Master exitosa. IP: %s, Puerto: %s", ip, puerto);
	
    handshake_query(conexion_master);
	
    int id_recibido;
    int bytes = recv(conexion_master, &id_recibido,sizeof(int), 0);
    if(bytes <= 0){
        log_warning(logger, "## Error al designar un id.");
        return -1;
    }
	
    id_designado = id_recibido;

    enviar_query_a_master(conexion_master, id_designado, argv);
	
    log_info(logger, "## Solicitud de ejecución de Query: %s. ID. %d. Prioridad: %s", argv[2],id_designado, argv[3]);

	while(1){
	    t_list* respuesta_master = recibir_paquete(conexion_master);
	    t_master_op op = *(t_master_op*) list_get(respuesta_master, 0);

        if (respuesta_master == NULL) {
            log_error(logger, "El Master se desconectó o falló la recepción.");
            terminar_programa(conexion_master, logger, config);
            return EXIT_FAILURE; 
        }

        if (list_size(respuesta_master) == 0) {
            log_warning(logger, "Se recibió un paquete vacío.");
            list_destroy(respuesta_master);
            return EXIT_FAILURE; 
        }
		
	    int id_query;
	    char* valor;
	    char* motivo;
	    char* nombre_archivo;
	    char* tag;
	    int pc;
		
	     switch (op) {
	            case LECTURA: 
	                id_query  = *(int*) list_get(respuesta_master, 1);   
	                valor = list_get(respuesta_master, 2);
	                nombre_archivo = list_get(respuesta_master, 3);
	                tag = list_get(respuesta_master, 4);
	
	                log_info(logger, "Lectura realizada: File %s:%s, contenido: %s", nombre_archivo,tag, valor);
	                break;
	            case DESALOJO:
	                id_query = *(int*)list_get(respuesta_master, 1);
	                nombre_archivo = list_get(respuesta_master, 2);
	                pc = *(int*)list_get(respuesta_master, 3);
	                motivo = list_get(respuesta_master, 4);
					
	                log_info(logger, "##Query: %d - Archivo: %s - PC: %d - Motivo: %s", id_query, nombre_archivo, pc, motivo);
	                break;            
	            case FIN:
	                id_query = *(int*)list_get(respuesta_master, 1);
	                nombre_archivo = list_get(respuesta_master, 2);
	                motivo = list_get(respuesta_master, 3);
					
	                log_info(logger, "## Query Finalizada - %s", motivo);
					
	                list_destroy_and_destroy_elements(respuesta_master, free);
	                terminar_programa(conexion_master, logger, config);
	                return 0;
	            case DESCONEXION_WORKER:
	                id_query = *(int*)list_get(respuesta_master, 1);
	                nombre_archivo = list_get(respuesta_master, 2);
	                motivo = list_get(respuesta_master, 3);
					
	                //log_info(logger, "##Query: %d - Archivo: %s - Motivo: %s", id_query, nombre_archivo, motivo);
	                log_info(logger, "## Query Finalizada - %s", motivo);
					
	                list_destroy_and_destroy_elements(respuesta_master, free);
	                terminar_programa(conexion_master, logger, config);
			 		return 0;
	            default:
	                log_error(logger,"La query recibio desde master %d elementos", list_size(respuesta_master));
					list_destroy_and_destroy_elements(respuesta_master, free);
	                abort();
	            }
		list_destroy_and_destroy_elements(respuesta_master, free);
		}
	
	    terminar_programa(conexion_master, logger, config);
	    return 0;
}

void enviar_query_a_master(int conexion_master,int id, char* argv[])
{
	t_paquete* paquete = crear_paquete();
    t_master_op op = EJECUTAR_QUERY;
    char* archivo_query = argv[2];
    char* prioridad_str = argv[3];
    agregar_a_paquete(paquete, &op, sizeof(t_master_op));
    agregar_a_paquete(paquete, &id, sizeof(int));
	agregar_a_paquete(paquete, archivo_query, strlen(archivo_query) + 1);
    agregar_a_paquete(paquete, prioridad_str, strlen(prioridad_str) + 1);

	enviar_paquete(paquete, conexion_master);

    eliminar_paquete(paquete);
}

void terminar_programa(int conexion_master, t_log* logger, t_config* config)
{
	close(conexion_master);
	log_destroy(logger);
	config_destroy(config);
}

int handshake_query(int conexion)
{
    int i = 20;
    if(send(conexion,&i,sizeof(int),0)<=0){
        log_error(logger, "Error al enviar handshake query-master");
        return -1;
    }
	int ok;
    int bytes = recv(conexion,&ok,sizeof(int),MSG_WAITALL);
    if (bytes <= 0){
    log_error(logger, "error al recibir el handshake query-master");
    return -1;
    }
    if(ok >= 0){
        log_info(logger,"handshake Query realizado: %d", ok);
        return ok;
    } else{ 
        log_error(logger,"handshake Query fallido");
        return -1;
    }
}

void terminar_query_abruptamente(int signal){
    t_master_op op_master = DESCONEXION_QUERY;
    t_paquete* desconexion = crear_paquete();
    agregar_a_paquete(desconexion, &op_master, sizeof(t_master_op));
    agregar_a_paquete(desconexion, &id_designado, sizeof(int));    
    enviar_paquete(desconexion, conexion_master);
    eliminar_paquete(desconexion);
    terminar_programa(conexion_master, logger, config);
    exit(0);
}
