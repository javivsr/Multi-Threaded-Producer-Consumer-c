/*
    * @author Enrique Quiñones Ureta 71179172Z
    * @author Javier Vaca Sánchez 71205788L
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>
#include <semaphore.h>

// Número de tipos de productos ('a'..'j')
#define NUM_PROD_TIPOS 10
// Límites de los parámetros
#define MAX_BUFFER_TAM 5000
#define MAX_PROVIDERS 7
#define MAX_CONSUMERS 1000

// Registro del buffer
typedef struct {
    char producto;
    int proveedor;
} CircularBuffer;

// Nodo de lista enlazada
typedef struct NodeList {
    int cliente;
    int consumedProducts;
    int typeConsumedProducts[NUM_PROD_TIPOS];
    int provConsumedProducts[MAX_PROVIDERS];
    struct NodeList * next;
} NodeList_t;

// Estructura de datos para los proveedores
typedef struct {
    int id;  // Identificador de proveedor
    int T;   // Tamaño del buffer circular
    const char *path;  // Directorio ficheros entrada
    const char *outputFile;  // Nombre fichero salida
} ProviderData;

// Estructura de datos para los clientes
typedef struct {
    int id; // Identificador de cliente
    int T;  // Tamaño del buffer circular
} ConsumerData;

// Estructura de datos para el facturador
typedef struct {
    int P;  // Numero de proveedores
    const char *outputFile;  // Nombre fichero salida 
} BillerData;


// ########## VARIABLES GLOBALES ##########

// Buffer circular
int sig_llenar = 0;         // Indice de escritura
int sig_vaciar = 0;         // Indice de lectura
CircularBuffer *B = NULL;   // Array (dinámico) del buffer
// Lista enlazada
NodeList_t *lis_ini = NULL; // Primer nodo lista
NodeList_t *lis_fin = NULL; // Ultimo nodo lista
// Variables que indican que no se van a escribir mas datos..
bool fin_proveedores = false;  // ..en el buffer circular 
// Indica numero de datos restantes en buffer
// (= semáforo hay_dato_buf_o_fin, pero no podemos "leer" valor de semáforo)
int num_buf = 0; 
// Semáforos
sem_t mutex_fich;          // Mutex para escritura en fichero de salida
sem_t mutex_esc_buffer;    // Mutex para escribir en buffer
sem_t mutex_lee_buffer;    // Mutex para leer de buffer
sem_t hay_dato_buf_o_fin;  // Nº Datos disponibles para lectura buffer circular o fin de datos
sem_t hay_espacio;         // Nº Huecos disponibles para escritura buffer circular
sem_t mutex_esc_lista;     // Mutex para escribir en lista
sem_t hay_dato_lis_o_fin;  // Nº Datos disponibles para lectura lista o fin de datos
sem_t mutex_num_buf;       // Mutex para actualizar num_buf
        
// #########################
// ### HILO DE PROVEEDOR ###
// #########################
void *providerThread(void *arg) {
    ProviderData *data = (ProviderData *) arg;

    char archivo[1024];
    char msgError[1124];
    CircularBuffer p; // Variable local para el dato de tipo CircularBuffer

    // Abrimos el fichero correspondiente del proveedor en cuestión
    sprintf(archivo, "%s/proveedor%d.dat", data->path, data->id);
    FILE *file = fopen(archivo, "r");

    if(file == NULL) {
        sprintf(msgError, "Error: No se ha podido abrir %s", archivo);
        perror(msgError);
        pthread_exit(NULL);
    }

    // Leemos el fichero del proveedor carácter a carácter
    // Truco de C para inicializar posiciones a cero
    int validProducts[NUM_PROD_TIPOS] = {0}; // Frecuencia de cada tipo de producto
    int totalValidProducts = 0;
    int processedProducts = 0;
    char producto = '0';
    int iproducto;
    while(fscanf(file, "%c", &producto) == 1) {
        iproducto = producto - 'a';
        if(iproducto >= 0 && iproducto < NUM_PROD_TIPOS) {
            // Producto válido, incrementamos total y por producto
            totalValidProducts++;
            validProducts[iproducto]++;
            p.producto = producto;
            p.proveedor = (data->id);
            // Añadir dato en el buffer
            sem_wait(&hay_espacio); // Espera por si no hay espacio libre
            sem_wait(&mutex_esc_buffer); // Acceso excluyente escritura buffer
                // Sección Crítica: Solo un proovedor a la vez
                B[sig_llenar] = p;
                sig_llenar = (sig_llenar + 1) % data->T;
                // Incrementamos variable global numero de datos en buffer
                sem_wait(&mutex_num_buf); 
                    // Sección Crítica porque acceden clientes y proovedores
                    num_buf++;
                sem_post(&mutex_num_buf);
            sem_post(&mutex_esc_buffer); // Liberamos acceso escritura buffer
            sem_post(&hay_dato_buf_o_fin); // Indicamos a clientes que hay dato
        }
        processedProducts++;
    }
    fclose(file);

    // Escritura en el fichero de salida
    sem_wait(&mutex_fich); // Acceso excluyente a fichero salida
        FILE *outputfile = fopen(data->outputFile, "a");
        fprintf(outputfile, "Proveedor %d:\n", data->id);
        fprintf(outputfile, "  Productos procesados: %d.\n", processedProducts);
        fprintf(outputfile, "  Productos inválidos: %d.\n", processedProducts - totalValidProducts);
        fprintf(outputfile, "  Productos validos: %d. De los cuales se han insertado:\n", totalValidProducts);
        // Escribimos los resultadas para cada tipo en el archivo
        for(int i = 0; i < NUM_PROD_TIPOS; i++) {
            fprintf(outputfile, "    %d de tipo \"%c\".\n", validProducts[i], 'a' + i);
        }
        fclose(outputfile);
    sem_post(&mutex_fich); // Liberamos acceso fichero salida
    // Salimos del hilo
    pthread_exit(NULL);
}

// #######################
// ### HILO DE CLIENTE ###
// #######################
void *consumerThread(void *arg) {
    ConsumerData *data = (ConsumerData *) arg;

    CircularBuffer e; // Variable local para el dato de tipo CircularBuffer
    int typeConsumedProducts[NUM_PROD_TIPOS] = {0}; // Frecuencia de cada tipo de producto
    int provConsumedProducts[MAX_PROVIDERS] = {0}; // Frecuencia de productos de cada proveedor
    int consumedProducts = 0;  // Numero total de productos consumidos

    // Bucle de lectura de productos del buffer circular
    while(true) {
        sem_wait(&hay_dato_buf_o_fin); // Hay que esperar si no hay datos
        sem_wait(&mutex_lee_buffer); // Acceso excluyente lectura buffer
            // En este código solo hay un cliente a la vez
            if(fin_proveedores && num_buf <= 0) { 
                // La señal de último dato era para señalar final, no porque hubiera datos
                sem_post(&hay_dato_buf_o_fin); // La activamos para siguiente cliente
                sem_post(&mutex_lee_buffer);   // Liberar acceso lectura buffer
                break;  // Nos salimos del bucle
            }            
            // Consumimos un producto del buffer circular
            e = B[sig_vaciar];
            sig_vaciar = (sig_vaciar + 1) % data->T;
            sem_wait(&mutex_num_buf); // Decrementamos numero de datos
                num_buf--;
            sem_post(&mutex_num_buf);
        sem_post(&mutex_lee_buffer); // Liberar acceso lectura buffer
        sem_post(&hay_espacio); // Señalamos a proveedores hay hueco en buffer
        // Incrementamos contadores de los productos consumidos
        typeConsumedProducts[e.producto - 'a']++;
        provConsumedProducts[e.proveedor]++;
        consumedProducts++;
    }

    // Añadir datos a la lista enlazada
    // Creación del nodo
    NodeList_t *nodo = (NodeList_t *) malloc(sizeof(NodeList_t));
    nodo->cliente = data->id;
    nodo->consumedProducts = consumedProducts;
    for(int i = 0; i < NUM_PROD_TIPOS; i++) {
        nodo->typeConsumedProducts[i] = typeConsumedProducts[i];
    }
    for(int i = 0; i < MAX_PROVIDERS; i++) {
        nodo->provConsumedProducts[i] = provConsumedProducts[i];
    }
    nodo->next = NULL;
    // Añadir nodo al final de la lista
    sem_wait(&mutex_esc_lista); // Acceso excluyente escritura lista
        if(lis_fin == NULL) {
            lis_ini = nodo;
            lis_fin = nodo;
        } else {
            lis_fin->next = nodo;
            lis_fin = nodo;
        }        
    sem_post(&mutex_esc_lista); // Liberar acceso escritura lista
    sem_post(&hay_dato_lis_o_fin); // Señalamos al facturador que hay nuevos datos
    //  Salimos del hilo
    pthread_exit(NULL);    
}

// ##########################
// ### HILO DE FACTURADOR ###
// ##########################
void *billerThread(void *arg) {
    BillerData *data = (BillerData *) arg;

    int provConsumedProducts[MAX_PROVIDERS] = {0}; // Frecuencia de productos de cada proveedor
    int consumedProducts = 0; // Numero total de productos consumidos
    int maxClientId = -1; // Indice cliente con más productos consumidos
    int maxClientProds = -1; // Nº productos del cliente con más productos consumidos
    // Escritura en el fichero de salida
    // Nota: No se proteje porque los proveedores ya han terminado
    FILE *file = fopen(data->outputFile, "a");
    fprintf(file, "\n");
    while(true) {
        sem_wait(&hay_dato_lis_o_fin); // Esperar si no hay datos
        // Si la lista está vacía significa que ya se han procesado todos
        // los datos (era la ultima señal "fake")
        if(lis_ini == NULL) { break; }

        fprintf(file, "Cliente consumidor %d:\n", lis_ini->cliente);
        fprintf(file, "  Productos consumidos: %d. De los cuales:\n", lis_ini->consumedProducts);
        for(int i = 0; i < NUM_PROD_TIPOS; i++) {
            fprintf(file, "    Producto tipo \"%c\": %d\n", 'a' + i, lis_ini->typeConsumedProducts[i]);
        }
        // Incrementamos totales por proovedor
        for(int p = 0; p < data->P; p++) {
            provConsumedProducts[p] += lis_ini->provConsumedProducts[p];
        }
        consumedProducts += lis_ini->consumedProducts;
        // Detectamos cliente máximo
        if(lis_ini->consumedProducts > maxClientProds) {
            maxClientProds = lis_ini->consumedProducts;
            maxClientId = lis_ini->cliente;
        }
        sem_wait(&mutex_esc_lista); // Acceso excluyente escritura lista
            NodeList_t *ant = lis_ini;
            lis_ini = lis_ini->next;
            if(lis_ini == NULL) {
                lis_fin = NULL;
            }
            free(ant);  // Vamos liberando la lista a medida que la leemos
        sem_post(&mutex_esc_lista); // Liberar acceso escritura lista            
    }
    // Escribir total
    fprintf(file, "\nTotal de productos consumidos: %d.\n", consumedProducts);
    // Escribir proveedores
    for(int p = 0; p < data->P; p++) {
        fprintf(file, "  %d del proveedor %d.\n", provConsumedProducts[p], p);
    }
    // Escribir mejor cliente
    fprintf(file, "Cliente consumidor que mas ha consumido: %d.\n", maxClientId);
    fclose(file);
    //  Salimos del hilo
    pthread_exit(NULL);
}

// ##########################
// ### PROGRAMA PRINCIPAL ###
// ##########################
int main(int argc, char* argv[]) {
    // Comprobación de argumentos de entrada
    if(argc != 6) {
        fprintf(stderr, "Asegúrese de cumplir el patrón: %s <path> <outputFile> <T> <P> <C>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int T = atoi(argv[3]);
    int P = atoi(argv[4]);
    int C = atoi(argv[5]);
    const char *path = argv[1];
    const char *outputFile = argv[2];

    if(T < 1 || T > MAX_BUFFER_TAM) {
        perror("Error: El tamaño del buffer debe estar comprendido entre el 1 y el 5000.\n");
        exit(EXIT_FAILURE);
    }
    if(P < 1 || P > MAX_PROVIDERS) {
        perror("Error: El número de proveedores debe estar comprendido entre el 1 y el 7.\n");
        exit(EXIT_FAILURE);
    }
    if(C < 1 || C > MAX_CONSUMERS) {
        perror("Error: El número de consumidores debe estar comprendido entre el 1 y el 1000.\n");
        exit(EXIT_FAILURE);
    }

    // Inicialización semáforos
    sem_init(&mutex_fich, 0, 1);
    sem_init(&mutex_esc_buffer, 0, 1);
    sem_init(&mutex_lee_buffer, 0, 1);
    sem_init(&hay_dato_buf_o_fin, 0, 0);
    sem_init(&hay_espacio, 0, T);
    sem_init(&mutex_esc_lista, 0, 1);
    sem_init(&hay_dato_lis_o_fin, 0, 0);
    sem_init(&mutex_num_buf, 0, 1);

    // Creamos el buffer circular
    B = (CircularBuffer *) malloc(sizeof(CircularBuffer) * T);
    if(B == NULL) {
        perror("Error: No se pudo asignar memoria.\n");
        exit(EXIT_FAILURE);
    }

    // Creamos y asignamos los argumentos para hilos de proveedores
    ProviderData *vp = (ProviderData *) malloc(P * sizeof(ProviderData));
    if(vp == NULL) {
        perror("Error: No se pudo asignar memoria.\n");
        exit(EXIT_FAILURE);
    }
    for(int i = 0; i < P; i++) {
        vp[i].id = i;
        vp[i].T = T;
        vp[i].path = path;
        vp[i].outputFile = outputFile;
    }
    // Creamos y asignamos los argumentos para hilos de clientes
    ConsumerData *vc = (ConsumerData *) malloc(C * sizeof(ConsumerData));
    if(vc == NULL) {
        perror("Error: No se pudo asignar memoria.\n");
        exit(EXIT_FAILURE);
    }
    for(int i = 0; i < C; i++) {
        vc[i].id = i;
        vc[i].T = T;
    }
    // Declaramos argumentos para hilo de facturador
    BillerData billerData;
    billerData.P = P;
    billerData.outputFile = outputFile;

    // Creamos arrays de hilos de proveedores y de clientes
    pthread_t *vhp = (pthread_t *) malloc(P * sizeof(pthread_t));
    pthread_t *vhc = (pthread_t *) malloc(C * sizeof(pthread_t));
    if(vhp == NULL || vhc == NULL) {
        perror("Error: No se pudo asignar memoria.\n");
        exit(EXIT_FAILURE);
    }

    // Lanzamos hilos de Proveedores
    for(int i = 0; i < P; i++) { 
        // Comprobamos que todo haya ido bien a la hora de crear el hilo
        if(pthread_create(&vhp[i], NULL, providerThread, (void *) &vp[i]) != 0) {
            perror("Error creando hilo de proveedor.\n");
            exit(EXIT_FAILURE);        
        }
    }

    // Lanzamos hilos de Clientes
    for(int i = 0; i < C; i++) { 
        // Comprobamos que todo haya ido bien a la hora de crear el hilo
        if(pthread_create(&vhc[i], NULL, consumerThread, (void *) &vc[i]) != 0) {
            perror("Error creando hilo de cliente.\n");
            exit(EXIT_FAILURE);        
        }
    }

    // Esperamos a que terminen todos los proveedores
    for(int i = 0; i < P; i++) {
        pthread_join(vhp[i], NULL);
    }

    // Avisamos a los clientes de que ya no se van a insertar mas datos
    fin_proveedores = true;
    // Mandamos señal de fin de datos, desbloquea cliente esperando
    sem_post(&hay_dato_buf_o_fin); 

    // Lanzamos hilo de Facturador (tras terminar los proovedores,
    // porque tiene que escribir en el fichero de salida)
    pthread_t vhf;    
    if(pthread_create(&vhf, NULL, billerThread, (void *) &billerData) != 0) {
        perror("Error creando hilo de facturador.\n");
        exit(EXIT_FAILURE);            
    }

    // Esperamos a que terminen todos los clientes
    for(int i = 0; i < C; i++) {
        pthread_join(vhc[i], NULL);
    }
    // Se libera a facturador señalando un dato 'fake',
    // detectará que no hay dato porque lis_ini == NULL
    sem_post(&hay_dato_lis_o_fin); 

    // Esperamos a que termine facturador
    pthread_join(vhf, NULL);

    // Destrucción de semáforos
    sem_destroy(&mutex_fich);
    sem_destroy(&mutex_esc_buffer);
    sem_destroy(&mutex_lee_buffer);
    sem_destroy(&hay_dato_buf_o_fin);
    sem_destroy(&hay_espacio);
    sem_destroy(&mutex_esc_lista);
    sem_destroy(&hay_dato_lis_o_fin);
    sem_destroy(&mutex_num_buf);
    // Destrucción de arrays
    // Nota: La lista ha sido destruida por facturador
    free(B);
    free(vp);
    free(vhp);
    free(vc);
    free(vhc);
    return 0; // Termina el programa con éxito
}