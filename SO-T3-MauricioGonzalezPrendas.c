/**
 * Instituto Tecnológico de Costa Rica
 * Campus Tecnológico Central Cartago
 * Escuela de Ingeniería en Computación
 * Curso: Principios de Sistemas Operativos
 * Periodo: II Semestre, 2026
 * Profesor: M.Sc. Esteban Arias Méndez
 *
 * Tarea # 3: Box Mr. Meeseeks
 * Archivo: SO-T3-MauricioGonzalezPrendas.c
 * Estudiante: Mauricio González Prendas
 * Carné: 2024143009
 *
 * Compilación:
 *   gcc -Wall -Wextra -pedantic -std=c11 -D_GNU_SOURCE -pthread -O2 SO-T3-MauricioGonzalezPrendas.c -o SO-T3-MauricioGonzalezPrendas -pthread -lm
 *
 * Descripción general:
 *   Programa que modela el entorno interactivo
 *   de Box Mr. Meeseeks mediante la bifurcación de procesos
 *   estableciendo canales de comunicación privada con tuberías bidireccionales y
 *   coordinando contadores compartidos a través de semáforos y memoria compartida
 *   del sistema operativo. El flujo atiende consultas textuales basadas en simulación
 *   probabilística y dilución de dificultad por envejecimiento, resuelve expresiones
 *   aritméticas o lógicas mediante un proceso dedicado y ejecuta utilidades externas
 *   monitoreando su código de retorno, incorporando un protocolo ordenado de despedida
 *   en reversa, temporización límite para prevenir caos en el sistema y un informe
 *   consolidado con las métricas recopiladas durante la sesión.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <semaphore.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <math.h>
#include <ctype.h>
#include <poll.h>

/* constantes y definiciones globales */
#define MAX_LINE_LEN        1024
#define MAX_TASKS_HISTORY    128
#define MAX_CHILDREN_NODES   32
#define DEFAULT_CHAOS_TIMEOUT 300 /* 5 minutos por defecto */
#define CONTINGENCY_MAX_LEVEL 3   /* límite de niveles para tareas con dificultad 0 */

/* tipos de solicitudes */
typedef enum {
    TASK_CASE_A_SIMULATION = 1,
    TASK_CASE_B_CALCULATION,
    TASK_CASE_C_EXTERNAL_PROG
} TaskType;

/* mensajes entre procesos por tuberías */
typedef enum {
    MSG_TASK_ASSIGN = 1,   /* el padre asigna o transmite la tarea al hijo */
    MSG_TASK_SOLVED,       /* el hijo informa que resolvió la tarea con éxito */
    MSG_TASK_FAILED,       /* el hijo informa que no se pudo resolver */
    MSG_TERMINATE_ORDER,   /* orden del padre a los hermanos para terminar */
    MSG_FAREWELL_ACK       /* confirmación de despedida */
} PipeMsgType;

/* estructura del paquete transmitido por tuberías */
typedef struct {
    PipeMsgType type;
    TaskType task_type;
    int solver_pid;
    int solver_ppid;
    int solver_level;
    int solver_instance;
    double final_difficulty;
    double sim_duration;
    char text[512];
    double result_value;
    bool has_error;
} MeeseeksMsg;

/* estado global en memoria compartida protegido por semáforo */
typedef struct {
    sem_t mutex;                 /* semáforo para exclusión mutua de contadores */
    int meeseeks_count_current;  /* meeseeks creados en la tarea actual */
    int meeseeks_count_total;    /* total acumulado en la sesión */
    int max_level_reached;       /* nivel de profundidad máximo alcanzado */
    int solver_pid;              /* pid del meeseeks que resolvió */
    int solver_ppid;             /* ppid del meeseeks que resolvió */
    int solver_level;            /* nivel del resolutor */
    int solver_instance;         /* instancia del resolutor */
    bool task_solved;            /* bandera de tarea resuelta */
} SharedContext;

/* registro de estadísticas históricas de la sesión */
typedef struct {
    int task_id;
    TaskType type;
    char description[256];
    double real_time_sec;
    double simulated_wait_sec;
    bool completed;
    bool planetary_chaos;
    int meeseeks_created;
    int max_level;
    int solver_pid;
    int solver_ppid;
    int solver_level;
    int solver_instance;
} TaskRecord;

/* tuberías privadas bidireccionales entre un meeseeks padre y su hijo */
typedef struct {
    int to_child[2];   /* padre escribe en [1], hijo lee de [0] */
    int to_parent[2];  /* hijo escribe en [1], padre lee de [0] */
    pid_t pid;
    int instance;
} ChildPipe;

/* variables globales de control en el proceso box */
static SharedContext *g_shm = NULL;
static TaskRecord g_history[MAX_TASKS_HISTORY];
static int g_task_count = 0;
static pid_t g_current_root_pid = -1;
static volatile sig_atomic_t g_chaos_occurred = 0;
static int g_chaos_timeout_seconds = DEFAULT_CHAOS_TIMEOUT;

/* prototipos de funciones */
/* gestión de memoria compartida y sincronización */
static bool init_shared_memory(void);
static void cleanup_shared_memory(void);
static void shm_register_meeseeks_birth(int level);

/* manejadores de señales y temporizadores */
static void sigalrm_handler(int sig);
static void setup_signals(void);

/* utilidades de tiempo */
static double get_timestamp_sec(void);

/* lógica de meeseeks y procesos */
static void run_meeseeks_process(int level, int instance, int read_from_parent_fd,
                                int write_to_parent_fd, int contingency_depth);

/* casos funcionales */
static bool evaluate_arithmetic_logic(const char *expr, double *out_res, char *err_msg, size_t err_len);
static bool execute_external_program(const char *cmd_line, int *out_exit_code);

/* interfaz y ciclo de vida de box */
static void print_box_banner(void);
static void handle_user_request(const char *input_str);
static void print_final_summary_report(void);

/* implementación: memoria compartida y semáforos */

/**
 * Inicializa la memoria compartida anónima (mmap) y el semáforo POSIX.
 * Se usa pshared=1 para permitir exclusión mutua entre los procesos
 * bifurcados (referencia: Beginning Linux Programming, caps. 12 y 14).
 */
static bool init_shared_memory(void) {
    g_shm = (SharedContext *)mmap(NULL, sizeof(SharedContext),
                                  PROT_READ | PROT_WRITE,
                                  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (g_shm == MAP_FAILED) {
        perror("Error al mapear memoria compartida anónima");
        return false;
    }

    /* inicializa el semáforo con pshared=1 para compartirlo entre procesos hijos */
    if (sem_init(&g_shm->mutex, 1, 1) != 0) {
        perror("Error al inicializar semáforo POSIX");
        munmap(g_shm, sizeof(SharedContext));
        g_shm = NULL;
        return false;
    }

    g_shm->meeseeks_count_current = 0;
    g_shm->meeseeks_count_total = 0;
    g_shm->max_level_reached = 1;
    g_shm->solver_pid = -1;
    g_shm->solver_ppid = -1;
    g_shm->solver_level = -1;
    g_shm->solver_instance = -1;
    g_shm->task_solved = false;

    return true;
}

/**
 * Libera el semáforo y desmapea la memoria compartida.
 */
static void cleanup_shared_memory(void) {
    if (g_shm != NULL) {
        sem_destroy(&g_shm->mutex);
        munmap(g_shm, sizeof(SharedContext));
        g_shm = NULL;
    }
}

/**
 * Registra el nacimiento de un nuevo Mr. Meeseeks incrementando los contadores
 * y actualizando el nivel máximo alcanzado de forma atómica.
 */
static void shm_register_meeseeks_birth(int level) {
    if (g_shm == NULL) return;
    sem_wait(&g_shm->mutex);
    g_shm->meeseeks_count_current++;
    g_shm->meeseeks_count_total++;
    if (level > g_shm->max_level_reached) {
        g_shm->max_level_reached = level;
    }
    sem_post(&g_shm->mutex);
}

/* implementación: control de señales y caos planetario */

/**
 * Manejador de SIGALRM para activar el caos planetario cuando
 * una tarea excede el tiempo límite establecido.
 */
static void sigalrm_handler(int sig) {
    (void)sig;
    g_chaos_occurred = 1;
}

/**
 * Configuración de señales para el proceso principal.
 */
static void setup_signals(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigalrm_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGALRM, &sa, NULL);

    /* ignorar SIGPIPE para manejar desconexiones de tuberías mediante errno */
    signal(SIGPIPE, SIG_IGN);
}

/**
 * Retorna la marca de tiempo actual en segundos con precisión de nanosegundos.
 */
static double get_timestamp_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ((double)ts.tv_nsec / 1.0e9);
}

/* caso b: evaluador de expresiones aritméticas y lógicas */

/**
 * Parser recursivo descendente con soporte para operadores aritméticos y lógicos.
 */
typedef struct {
    const char *src;
    size_t pos;
} ParserState;

static void skip_whitespace(ParserState *p) {
    while (p->src[p->pos] && isspace((unsigned char)p->src[p->pos])) {
        p->pos++;
    }
}

static bool parse_logical_or(ParserState *p, double *res, char *err, size_t err_len);
static bool parse_logical_and(ParserState *p, double *res, char *err, size_t err_len);
static bool parse_equality(ParserState *p, double *res, char *err, size_t err_len);
static bool parse_relational(ParserState *p, double *res, char *err, size_t err_len);
static bool parse_addition(ParserState *p, double *res, char *err, size_t err_len);
static bool parse_multiplication(ParserState *p, double *res, char *err, size_t err_len);
static bool parse_unary(ParserState *p, double *res, char *err, size_t err_len);
static bool parse_primary(ParserState *p, double *res, char *err, size_t err_len);

static bool parse_primary(ParserState *p, double *res, char *err, size_t err_len) {
    skip_whitespace(p);
    char c = p->src[p->pos];

    if (c == '(') {
        p->pos++;
        if (!parse_logical_or(p, res, err, err_len)) return false;
        skip_whitespace(p);
        if (p->src[p->pos] != ')') {
            snprintf(err, err_len, "Paréntesis de cierre ')' faltante en posición %zu", p->pos);
            return false;
        }
        p->pos++;
        return true;
    }

    if (isdigit((unsigned char)c) || c == '.') {
        char *endptr = NULL;
        *res = strtod(&p->src[p->pos], &endptr);
        if (endptr == &p->src[p->pos]) {
            snprintf(err, err_len, "Error al procesar número en posición %zu", p->pos);
            return false;
        }
        p->pos = (size_t)(endptr - p->src);
        return true;
    }

    snprintf(err, err_len, "Carácter inesperado '%c' en posición %zu", c, p->pos);
    return false;
}

static bool parse_unary(ParserState *p, double *res, char *err, size_t err_len) {
    skip_whitespace(p);
    if (p->src[p->pos] == '!') {
        p->pos++;
        if (!parse_unary(p, res, err, err_len)) return false;
        *res = (*res == 0.0) ? 1.0 : 0.0;
        return true;
    }
    if (p->src[p->pos] == '+') {
        p->pos++;
        return parse_unary(p, res, err, err_len);
    }
    if (p->src[p->pos] == '-') {
        p->pos++;
        if (!parse_unary(p, res, err, err_len)) return false;
        *res = -(*res);
        return true;
    }
    return parse_primary(p, res, err, err_len);
}

static bool parse_multiplication(ParserState *p, double *res, char *err, size_t err_len) {
    if (!parse_unary(p, res, err, err_len)) return false;
    while (true) {
        skip_whitespace(p);
        char c = p->src[p->pos];
        if (c == '*' || c == '/' || c == '%') {
            p->pos++;
            double right = 0.0;
            if (!parse_unary(p, &right, err, err_len)) return false;
            if (c == '*') {
                *res *= right;
            } else if (c == '/') {
                if (fabs(right) < 1e-12) {
                    snprintf(err, err_len, "Error matemático: División entre cero");
                    return false;
                }
                *res /= right;
            } else {
                if (fabs(right) < 1e-12) {
                    snprintf(err, err_len, "Error matemático: Módulo entre cero");
                    return false;
                }
                *res = fmod(*res, right);
            }
        } else {
            break;
        }
    }
    return true;
}

static bool parse_addition(ParserState *p, double *res, char *err, size_t err_len) {
    if (!parse_multiplication(p, res, err, err_len)) return false;
    while (true) {
        skip_whitespace(p);
        char c = p->src[p->pos];
        if (c == '+' || c == '-') {
            p->pos++;
            double right = 0.0;
            if (!parse_multiplication(p, &right, err, err_len)) return false;
            if (c == '+') *res += right;
            else *res -= right;
        } else {
            break;
        }
    }
    return true;
}

static bool parse_relational(ParserState *p, double *res, char *err, size_t err_len) {
    if (!parse_addition(p, res, err, err_len)) return false;
    while (true) {
        skip_whitespace(p);
        if (p->src[p->pos] == '<' && p->src[p->pos + 1] == '=') {
            p->pos += 2;
            double right = 0.0;
            if (!parse_addition(p, &right, err, err_len)) return false;
            *res = (*res <= right) ? 1.0 : 0.0;
        } else if (p->src[p->pos] == '>' && p->src[p->pos + 1] == '=') {
            p->pos += 2;
            double right = 0.0;
            if (!parse_addition(p, &right, err, err_len)) return false;
            *res = (*res >= right) ? 1.0 : 0.0;
        } else if (p->src[p->pos] == '<') {
            p->pos++;
            double right = 0.0;
            if (!parse_addition(p, &right, err, err_len)) return false;
            *res = (*res < right) ? 1.0 : 0.0;
        } else if (p->src[p->pos] == '>') {
            p->pos++;
            double right = 0.0;
            if (!parse_addition(p, &right, err, err_len)) return false;
            *res = (*res > right) ? 1.0 : 0.0;
        } else {
            break;
        }
    }
    return true;
}

static bool parse_equality(ParserState *p, double *res, char *err, size_t err_len) {
    if (!parse_relational(p, res, err, err_len)) return false;
    while (true) {
        skip_whitespace(p);
        if (p->src[p->pos] == '=' && p->src[p->pos + 1] == '=') {
            p->pos += 2;
            double right = 0.0;
            if (!parse_relational(p, &right, err, err_len)) return false;
            *res = (fabs(*res - right) < 1e-9) ? 1.0 : 0.0;
        } else if (p->src[p->pos] == '!' && p->src[p->pos + 1] == '=') {
            p->pos += 2;
            double right = 0.0;
            if (!parse_relational(p, &right, err, err_len)) return false;
            *res = (fabs(*res - right) >= 1e-9) ? 1.0 : 0.0;
        } else {
            break;
        }
    }
    return true;
}

static bool parse_logical_and(ParserState *p, double *res, char *err, size_t err_len) {
    if (!parse_equality(p, res, err, err_len)) return false;
    while (true) {
        skip_whitespace(p);
        if (p->src[p->pos] == '&' && p->src[p->pos + 1] == '&') {
            p->pos += 2;
            double right = 0.0;
            if (!parse_equality(p, &right, err, err_len)) return false;
            *res = (*res != 0.0 && right != 0.0) ? 1.0 : 0.0;
        } else {
            break;
        }
    }
    return true;
}

static bool parse_logical_or(ParserState *p, double *res, char *err, size_t err_len) {
    if (!parse_logical_and(p, res, err, err_len)) return false;
    while (true) {
        skip_whitespace(p);
        if (p->src[p->pos] == '|' && p->src[p->pos + 1] == '|') {
            p->pos += 2;
            double right = 0.0;
            if (!parse_logical_and(p, &right, err, err_len)) return false;
            *res = (*res != 0.0 || right != 0.0) ? 1.0 : 0.0;
        } else {
            break;
        }
    }
    return true;
}

static bool evaluate_arithmetic_logic(const char *expr, double *out_res, char *err_msg, size_t err_len) {
    ParserState p = { .src = expr, .pos = 0 };
    if (!parse_logical_or(&p, out_res, err_msg, err_len)) return false;
    skip_whitespace(&p);
    if (p.src[p.pos] != '\0') {
        snprintf(err_msg, err_len, "Sintaxis inválida en '%s' a partir de posición %zu", p.src, p.pos);
        return false;
    }
    return true;
}

/* caso c: ejecución de programas externos con fork y execvp */

/**
 * Ejecuta un programa externo del sistema esperando su código de retorno.
 * Basado en el manejo de procesos con fork y waitpid (Silberschatz cap. 3, BLP cap. 11).
 */
static bool execute_external_program(const char *cmd_line, int *out_exit_code) {
    char buf[512];
    strncpy(buf, cmd_line, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    char *argv[64];
    int argc = 0;
    char *token = strtok(buf, " \t\r\n");
    while (token != NULL && argc < 63) {
        argv[argc++] = token;
        token = strtok(NULL, " \t\r\n");
    }
    argv[argc] = NULL;

    if (argc == 0) {
        *out_exit_code = -1;
        return false;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("Error en fork() al invocar programa externo");
        *out_exit_code = -1;
        return false;
    }

    if (pid == 0) {
        /* proceso hijo: reemplaza su imagen con el ejecutable externo */
        execvp(argv[0], argv);
        /* si llega a este punto, execvp no pudo ejecutar el comando */
        fprintf(stderr, "[Mr. Meeseeks externo PID: %d] Error: no se pudo ejecutar '%s': %s\n",
                getpid(), argv[0], strerror(errno));
        _exit(127);
    }

    /* proceso padre: espera el resultado */
    int status = 0;
    if (waitpid(pid, &status, 0) == -1) {
        *out_exit_code = -1;
        return false;
    }

    if (WIFEXITED(status)) {
        *out_exit_code = WEXITSTATUS(status);
        return (*out_exit_code == 0);
    }

    *out_exit_code = -1;
    return false;
}

/* lógica de meeseeks: procesos concurrentes y despedida en reversa */

/**
 * Función principal ejecutada por cada Mr. Meeseeks instanciado.
 * Implementa el saludo obligatorio, la recepción de la tarea por tubería privada,
 * la evaluación de la solicitud, la bifurcación recursiva con pipes privados,
 * la dilución de dificultad y el protocolo de despedida en cascada reversa.
 */
static void run_meeseeks_process(int level, int instance, int read_from_parent_fd,
                                int write_to_parent_fd, int contingency_depth) {
    pid_t my_pid = getpid();
    pid_t my_ppid = getppid();

    /* 1. registrar nacimiento en memoria compartida */
    shm_register_meeseeks_birth(level);

    /* 2. mensaje de bienvenida estandarizado según requerimiento */
    printf("Hi, I'm Mr. Meeseeks! Look at Meeeee. (pid: %d, ppid: %d, N: %d, i: %d)\n",
           my_pid, my_ppid, level, instance);
    fflush(stdout);

    /* 3. recibir la tarea asignada mediante la tubería privada */
    MeeseeksMsg task_msg;
    memset(&task_msg, 0, sizeof(task_msg));
    if (read_from_parent_fd >= 0) {
        ssize_t n = read(read_from_parent_fd, &task_msg, sizeof(task_msg));
        if (n <= 0 || task_msg.type != MSG_TASK_ASSIGN) {
            if (write_to_parent_fd >= 0) close(write_to_parent_fd);
            if (read_from_parent_fd >= 0) close(read_from_parent_fd);
            _exit(1);
        }
    }

    TaskType ttype = task_msg.task_type;
    double difficulty = task_msg.final_difficulty;
    double sim_duration = task_msg.sim_duration;
    char task_str[512];
    snprintf(task_str, sizeof(task_str), "%s", task_msg.text);

    /* variables de resolución local */
    bool i_solved_it = false;
    bool child_solved_it = false;
    MeeseeksMsg solution_msg;
    memset(&solution_msg, 0, sizeof(solution_msg));

    if (ttype == TASK_CASE_B_CALCULATION) {
        /* caso b: cálculos aritméticos o lógicos ejecutados por un único meeseeks */
        double result = 0.0;
        char err_msg[256] = {0};
        bool ok = evaluate_arithmetic_logic(task_str, &result, err_msg, sizeof(err_msg));

        solution_msg.type = ok ? MSG_TASK_SOLVED : MSG_TASK_FAILED;
        solution_msg.solver_pid = my_pid;
        solution_msg.solver_ppid = my_ppid;
        solution_msg.solver_level = level;
        solution_msg.solver_instance = instance;
        solution_msg.result_value = result;
        solution_msg.has_error = !ok;

        if (ok) {
            snprintf(solution_msg.text, sizeof(solution_msg.text),
                     "Resultado: %.4f (o %lld si entero)", result, (long long)result);
            printf("[Mr. Meeseeks PID: %d, PPID: %d] Cálculo completado con éxito: '%s' = %.4f\n",
                   my_pid, my_ppid, task_str, result);
        } else {
            snprintf(solution_msg.text, sizeof(solution_msg.text), "%s", err_msg);
            printf("[Mr. Meeseeks PID: %d, PPID: %d] Error en cálculo: %s\n",
                   my_pid, my_ppid, err_msg);
        }
        fflush(stdout);
        i_solved_it = ok;

    } else if (ttype == TASK_CASE_C_EXTERNAL_PROG) {
        /* caso c: programas externos ejecutados por un único meeseeks */
        int exit_code = 0;
        printf("[Mr. Meeseeks PID: %d, PPID: %d] Ejecutando programa externo: '%s'...\n",
               my_pid, my_ppid, task_str);
        fflush(stdout);

        bool ok = execute_external_program(task_str, &exit_code);
        solution_msg.type = ok ? MSG_TASK_SOLVED : MSG_TASK_FAILED;
        solution_msg.solver_pid = my_pid;
        solution_msg.solver_ppid = my_ppid;
        solution_msg.solver_level = level;
        solution_msg.solver_instance = instance;
        solution_msg.has_error = !ok;

        if (ok) {
            snprintf(solution_msg.text, sizeof(solution_msg.text),
                     "Programa terminó con éxito (código de salida: 0)");
            printf("[Mr. Meeseeks PID: %d, PPID: %d] Programa completado con éxito (código: 0).\n",
                   my_pid, my_ppid);
        } else {
            snprintf(solution_msg.text, sizeof(solution_msg.text),
                     "Programa terminó con error (código de salida: %d)", exit_code);
            printf("[Mr. Meeseeks PID: %d, PPID: %d] Programa finalizó con errores (código: %d).\n",
                   my_pid, my_ppid, exit_code);
        }
        fflush(stdout);
        i_solved_it = ok;

    } else {
        /* caso a: simulación con dificultad, envejecimiento y bifurcación */
        bool terminated_early = false;
        if (sim_duration > 0.0 && read_from_parent_fd >= 0) {
            /* sondeo no bloqueante en la tubería del padre mientras reflexiona */
            struct pollfd pfd;
            pfd.fd = read_from_parent_fd;
            pfd.events = POLLIN;
            int timeout_ms = (int)(sim_duration * 1000.0);
            int pret = poll(&pfd, 1, timeout_ms);
            if (pret > 0 && (pfd.revents & POLLIN)) {
                MeeseeksMsg order;
                ssize_t r = read(read_from_parent_fd, &order, sizeof(order));
                if (r > 0 && order.type == MSG_TERMINATE_ORDER) {
                    terminated_early = true;
                }
            }
        } else if (sim_duration > 0.0) {
            useconds_t usec = (useconds_t)(sim_duration * 1000000.0);
            usleep(usec);
        }

        if (terminated_early) {
            /* un hermano ya resolvió la tarea mientras este meeseeks reflexionaba */
            printf("Hi, I'm Mr. Meeseeks! Mi existencia es dolor... ¡Me despido! (pid: %d, ppid: %d, N: %d, i: %d)\n",
                   my_pid, my_ppid, level, instance);
            fflush(stdout);

            if (write_to_parent_fd >= 0) {
                MeeseeksMsg ack = { .type = MSG_FAREWELL_ACK, .solver_pid = my_pid, .solver_ppid = my_ppid, .solver_level = level, .solver_instance = instance };
                write(write_to_parent_fd, &ack, sizeof(ack));
                close(write_to_parent_fd);
            }
            if (read_from_parent_fd >= 0) close(read_from_parent_fd);
            _exit(0);
        }

        /* evaluación de dificultad:
         * > 85: lo resuelve este meeseeks directamente
         * > 45: solicita ayuda a 2 meeseeks
         * <= 45: solicita ayuda a 5 meeseeks */
        if (difficulty > 85.0) {
            /* tarea suficientemente fácil para resolverla directamente */
            i_solved_it = true;
            solution_msg.type = MSG_TASK_SOLVED;
            solution_msg.solver_pid = my_pid;
            solution_msg.solver_ppid = my_ppid;
            solution_msg.solver_level = level;
            solution_msg.solver_instance = instance;
            solution_msg.final_difficulty = difficulty;
            snprintf(solution_msg.text, sizeof(solution_msg.text),
                     "¡Solicitud completada con éxito! (Dificultad final: %.2f)", difficulty);

            printf("[Mr. Meeseeks PID: %d, PPID: %d, N: %d, i: %d] ¡He resuelto la solicitud '%s'! (Dificultad resuelta: %.2f)\n",
                   my_pid, my_ppid, level, instance, task_str, difficulty);
            fflush(stdout);

        } else {
            /* tarea difícil: requiere invocar más meeseeks */
            int num_children = (difficulty > 45.0) ? 2 : 5;

            /* verificación de contingencia para dificultad 0 o exceso de profundidad */
            if (difficulty == 0.0 && level >= contingency_depth) {
                printf("[Contingencia PID: %d, PPID: %d, N: %d, i: %d] Dificultad 0 y límite de nivel (%d) alcanzado.\n"
                       "  => Deteniendo proliferación de procesos para prevenir colapso del sistema.\n",
                       my_pid, my_ppid, level, instance, contingency_depth);
                fflush(stdout);

                solution_msg.type = MSG_TASK_FAILED;
                solution_msg.solver_pid = my_pid;
                solution_msg.solver_ppid = my_ppid;
                solution_msg.solver_level = level;
                solution_msg.solver_instance = instance;
                snprintf(solution_msg.text, sizeof(solution_msg.text),
                         "Tarea imposible (dificultad 0). Abortada por plan de contingencia.");
                i_solved_it = false;
            } else {
                printf("[Mr. Meeseeks PID: %d, PPID: %d, N: %d, i: %d] Tarea difícil (Dif: %.2f). Solicitando ayuda a %d nuevos Mr. Meeseeks...\n",
                       my_pid, my_ppid, level, instance, difficulty, num_children);
                fflush(stdout);

                ChildPipe children[MAX_CHILDREN_NODES];
                memset(children, 0, sizeof(children));

                /* creación de tuberías privadas y bifurcación con fork */
                for (int c = 0; c < num_children; c++) {
                    int c_instance = c + 1;
                    if (pipe(children[c].to_child) == -1 || pipe(children[c].to_parent) == -1) {
                        perror("Error al crear tuberías privadas entre Meeseeks");
                        break;
                    }

                    pid_t c_pid = fork();
                    if (c_pid < 0) {
                        perror("Error en fork() al crear Meeseeks subordinado");
                        break;
                    }

                    if (c_pid == 0) {
                        /* proceso hijo meeseeks */
                        close(children[c].to_child[1]);
                        close(children[c].to_parent[0]);

                        /* cerrar tuberías heredadas de hermanos anteriores */
                        for (int prev = 0; prev < c; prev++) {
                            close(children[prev].to_child[0]);
                            close(children[prev].to_child[1]);
                            close(children[prev].to_parent[0]);
                            close(children[prev].to_parent[1]);
                        }
                        if (read_from_parent_fd >= 0) close(read_from_parent_fd);
                        if (write_to_parent_fd >= 0) close(write_to_parent_fd);

                        run_meeseeks_process(level + 1, c_instance,
                                             children[c].to_child[0],
                                             children[c].to_parent[1],
                                             contingency_depth);
                        _exit(0);
                    } else {
                        /* proceso padre meeseeks */
                        children[c].pid = c_pid;
                        children[c].instance = c_instance;
                        close(children[c].to_child[0]);  /* padre no lee de to_child */
                        close(children[c].to_parent[1]); /* padre no escribe en to_parent */

                        /* envejecimiento (aging): diluye la dificultad entre los procesos hijos */
                        double aged_diff = 0.0;
                        if (difficulty > 0.0) {
                            double boost = (100.0 - difficulty) / (double)num_children + (c * 2.5);
                            aged_diff = difficulty + boost;
                            if (aged_diff > 95.0) aged_diff = 95.0;
                        }

                        /* transmitir la tarea mediante la tubería privada al hijo */
                        MeeseeksMsg assign_msg;
                        memset(&assign_msg, 0, sizeof(assign_msg));
                        assign_msg.type = MSG_TASK_ASSIGN;
                        assign_msg.task_type = ttype;
                        assign_msg.final_difficulty = aged_diff;
                        assign_msg.sim_duration = sim_duration;
                        snprintf(assign_msg.text, sizeof(assign_msg.text), "%s", task_str);
                        write(children[c].to_child[1], &assign_msg, sizeof(assign_msg));
                    }
                }

                /* supervisión concurrente de las respuestas de los hijos con poll */
                struct pollfd pfds[MAX_CHILDREN_NODES];
                bool active[MAX_CHILDREN_NODES];
                for (int c = 0; c < num_children; c++) {
                    pfds[c].fd = children[c].to_parent[0];
                    pfds[c].events = POLLIN;
                    active[c] = true;
                }
                int remaining = num_children;
                int resolver_idx = -1;

                while (remaining > 0) {
                    int pret = poll(pfds, num_children, 200);
                    if (pret > 0) {
                        for (int c = 0; c < num_children; c++) {
                            if (active[c] && (pfds[c].revents & (POLLIN | POLLHUP))) {
                                MeeseeksMsg incoming;
                                ssize_t bytes = read(children[c].to_parent[0], &incoming, sizeof(incoming));
                                if (bytes > 0 && incoming.type == MSG_TASK_SOLVED) {
                                    child_solved_it = true;
                                    solution_msg = incoming;
                                    resolver_idx = c;
                                    active[c] = false;
                                    remaining--;
                                    break;
                                } else {
                                    active[c] = false;
                                    remaining--;
                                }
                            }
                        }
                        if (child_solved_it) {
                            /* notificar a los hermanos para que terminen y se despidan */
                            for (int c = 0; c < num_children; c++) {
                                if (c != resolver_idx && active[c]) {
                                    MeeseeksMsg term_order;
                                    memset(&term_order, 0, sizeof(term_order));
                                    term_order.type = MSG_TERMINATE_ORDER;
                                    write(children[c].to_child[1], &term_order, sizeof(term_order));
                                }
                            }
                            break;
                        }
                    }
                }

                /* esperar a todos los hijos para evitar procesos zombis */
                for (int c = 0; c < num_children; c++) {
                    waitpid(children[c].pid, NULL, 0);
                    close(children[c].to_child[1]);
                    close(children[c].to_parent[0]);
                }
            }
        }
    }

    /* mensajes de despedida y propagación hacia el ancestro */
    if (i_solved_it || child_solved_it) {
        /* despedida antes de desaparecer */
        printf("Hi, I'm Mr. Meeseeks! He cumplido mi propósito, gracias por permitirme servirle. ¡Me despido y desaparezco! (pid: %d, ppid: %d, N: %d, i: %d)\n",
               my_pid, my_ppid, level, instance);
        fflush(stdout);

        if (level == 1) {
            /* meeseeks raíz: guarda los datos del resolutor en memoria compartida */
            sem_wait(&g_shm->mutex);
            g_shm->task_solved = true;
            g_shm->solver_pid = solution_msg.solver_pid;
            g_shm->solver_ppid = solution_msg.solver_ppid;
            g_shm->solver_level = solution_msg.solver_level;
            g_shm->solver_instance = solution_msg.solver_instance;
            sem_post(&g_shm->mutex);

            printf("\n---------------------------------------------------------------\n");
            printf("[Mr. Meeseeks raíz PID: %d] ¡Misión cumplida!\n"
                   "  => Solucionado exitosamente por: Mr. Meeseeks (PID: %d, PPID: %d, N: %d, i: %d)\n"
                   "  => Detalle: %s\n",
                   my_pid, solution_msg.solver_pid, solution_msg.solver_ppid,
                   solution_msg.solver_level, solution_msg.solver_instance,
                   solution_msg.text);
            printf("---------------------------------------------------------------\n\n");
            fflush(stdout);
        }

        if (write_to_parent_fd >= 0) {
            write(write_to_parent_fd, &solution_msg, sizeof(solution_msg));
        }
    } else {
        /* caso de fallo o terminación ordenada por hermano */
        printf("Hi, I'm Mr. Meeseeks! Mi existencia es dolor... ¡Me despido! (pid: %d, ppid: %d, N: %d, i: %d)\n",
               my_pid, my_ppid, level, instance);
        fflush(stdout);

        if (level == 1) {
            sem_wait(&g_shm->mutex);
            g_shm->task_solved = false;
            sem_post(&g_shm->mutex);
        }

        if (write_to_parent_fd >= 0) {
            write(write_to_parent_fd, &solution_msg, sizeof(solution_msg));
        }
    }

    if (read_from_parent_fd >= 0) close(read_from_parent_fd);
    if (write_to_parent_fd >= 0) close(write_to_parent_fd);
}

/* interfaz y ciclo de vida de box */

static void print_box_banner(void) {
    printf("--------------------------------------------------------------------\n");
    printf("                   Meeseeks Box                \n");
    printf("           Principios de Sistemas Operativos           \n");
    printf("                   Mauricio González Prendas                                 \n");
    printf("--------------------------------------------------------------------\n");
    printf("Presione el botón de la Box ingresando su solicitud:\n");
    printf("  - Texto común:        ej. 'Quiero un café francés' o 'Quiero ser feliz'\n");
    printf("  - Operación o cálculo: ej. 'calc: (15 + 5) * 4 > 50 && 2 != 0'\n");
    printf("  - Programa externo:   ej. 'exec: uname -a' o 'exec: date'\n");
    printf("  - Configurar caos:    ej. 'config: timeout 10'\n");
    printf("  - Despedirse:         ej. 'adios', 'adiós', 'salir' o 'exit'\n");
    printf("--------------------------------------------------------------------\n\n");
}

static void handle_user_request(const char *input_str) {
    if (strlen(input_str) == 0) return;

    TaskType ttype = TASK_CASE_A_SIMULATION;
    char task_payload[MAX_LINE_LEN] = {0};
    double difficulty = 50.0;
    double sim_duration = 1.0;

    /* clasificación automática del comando */
    if (strncasecmp(input_str, "calc:", 5) == 0) {
        ttype = TASK_CASE_B_CALCULATION;
        snprintf(task_payload, sizeof(task_payload), "%s", input_str + 5);
    } else if (strncasecmp(input_str, "exec:", 5) == 0) {
        ttype = TASK_CASE_C_EXTERNAL_PROG;
        snprintf(task_payload, sizeof(task_payload), "%s", input_str + 5);
    } else if (strncasecmp(input_str, "config:", 7) == 0) {
        int new_timeout = 0;
        if (sscanf(input_str + 7, " timeout %d", &new_timeout) == 1 && new_timeout > 0) {
            g_chaos_timeout_seconds = new_timeout;
            printf("[Box Config] Tiempo máximo de caos planetario configurado en %d segundos.\n\n",
                   g_chaos_timeout_seconds);
        } else {
            printf("[Box Config] Uso: config: timeout <segundos>\n\n");
        }
        return;
    } else {
        /* caso a: consulta textual estándar */
        ttype = TASK_CASE_A_SIMULATION;
        snprintf(task_payload, sizeof(task_payload), "%s", input_str);

        printf("\n--- Opciones de simulación para la solicitud textual ---\n");
        printf("1. Consultar dificultad manualmente (0 a 100)\n");
        printf("2. Dejarlo a la suerte de Mr. Meeseeks (aleatorio estocástico)\n");
        printf("Seleccione opción (1 o 2) [Enter para 2]: ");
        char opt_buf[32];
        if (fgets(opt_buf, sizeof(opt_buf), stdin) && opt_buf[0] == '1') {
            printf("Ingrese la dificultad (0 = imposible, 100 = trivial): ");
            if (fgets(opt_buf, sizeof(opt_buf), stdin)) {
                difficulty = atof(opt_buf);
                if (difficulty < 0.0) difficulty = 0.0;
                if (difficulty > 100.0) difficulty = 100.0;
            }
        } else {
            /* estimación aleatoria */
            difficulty = (double)(rand() % 101);
            printf("  => Mr. Meeseeks estimó la dificultad en: %.1f / 100.0\n", difficulty);
        }

        /* tiempo de simulación en un rango de 0.5 a 5.0 segundos */
        sim_duration = 0.5 + ((double)(rand() % 46) / 10.0);
        printf("  => Tiempo simulado de reflexión: %.2f segundos\n\n", sim_duration);
    }

    /* reiniciar estado para la nueva tarea */
    sem_wait(&g_shm->mutex);
    g_shm->meeseeks_count_current = 0;
    g_shm->max_level_reached = 1;
    g_shm->task_solved = false;
    g_shm->solver_pid = -1;
    g_shm->solver_ppid = -1;
    g_shm->solver_level = -1;
    g_shm->solver_instance = -1;
    sem_post(&g_shm->mutex);

    g_chaos_occurred = 0;

    /* tuberías de comunicación entre box y meeseeks raíz */
    int box_to_root[2];
    int root_to_box[2];
    if (pipe(box_to_root) == -1 || pipe(root_to_box) == -1) {
        perror("Error al crear tuberías entre Box y Meeseeks Raíz");
        return;
    }

    printf("---------------------------------------------------------------\n");
    printf("¡Presionando el botón! Instanciando a Mr. Meeseeks...\n");
    printf("---------------------------------------------------------------\n");
    fflush(stdout);

    /* medición de tiempo de ejecución real */
    double t_start = get_timestamp_sec();

    /* activar temporizador de caos planetario */
    alarm(g_chaos_timeout_seconds);

    /* creación del proceso raíz mediante fork */
    pid_t root_pid = fork();
    if (root_pid < 0) {
        perror("Error en fork() del programa Box");
        alarm(0);
        close(box_to_root[0]); close(box_to_root[1]);
        close(root_to_box[0]); close(root_to_box[1]);
        return;
    }

    if (root_pid == 0) {
        /* proceso hijo: meeseeks raíz en primer nivel e instancia inicial */
        setpgid(0, 0);

        close(box_to_root[1]);
        close(root_to_box[0]);

        run_meeseeks_process(1, 1, box_to_root[0], root_to_box[1], CONTINGENCY_MAX_LEVEL);
        _exit(0);
    }

    /* proceso padre: box */
    g_current_root_pid = root_pid;
    close(box_to_root[0]);
    close(root_to_box[1]);

    /* transmitir la solicitud mediante la tubería privada al meeseeks raíz */
    MeeseeksMsg initial_req;
    memset(&initial_req, 0, sizeof(initial_req));
    initial_req.type = MSG_TASK_ASSIGN;
    initial_req.task_type = ttype;
    initial_req.final_difficulty = difficulty;
    initial_req.sim_duration = sim_duration;
    snprintf(initial_req.text, sizeof(initial_req.text), "%s", task_payload);
    write(box_to_root[1], &initial_req, sizeof(initial_req));

    /* supervisión de la ejecución o detección de caos planetario */
    int status = 0;
    bool is_chaos = false;

    while (true) {
        pid_t w = waitpid(root_pid, &status, WNOHANG);
        if (w == root_pid) {
            /* el proceso meeseeks terminó normalmente */
            break;
        } else if (w == -1) {
            if (errno == ECHILD) break;
            if (errno != EINTR) {
                perror("waitpid error");
                break;
            }
        }

        if (g_chaos_occurred) {
            is_chaos = true;
            printf("\n---------------------------------------------------------------\n");
            printf("            Alerta de caos planetario detectada                \n");
            printf("  El tiempo límite de %d s fue superado sin resolver la tarea. \n", g_chaos_timeout_seconds);
            printf("  Los Mr. Meeseeks no pueden morir y el caos se apodera del SO.\n");
            printf("  Terminando el árbol de procesos...\n");
            printf("---------------------------------------------------------------\n\n");
            fflush(stdout);

            /* terminar todo el grupo de procesos del árbol meeseeks */
            killpg(root_pid, SIGTERM);
            usleep(200000);
            killpg(root_pid, SIGKILL);
            waitpid(root_pid, &status, 0);
            break;
        }
        usleep(20000); /* 20 ms de sondeo no bloqueante */
    }

    /* desactivar alarma */
    alarm(0);

    /* leer respuesta del meeseeks raíz desde la tubería si no hubo caos */
    MeeseeksMsg final_root_msg;
    memset(&final_root_msg, 0, sizeof(final_root_msg));
    if (!is_chaos) {
        read(root_to_box[0], &final_root_msg, sizeof(final_root_msg));
    }
    close(box_to_root[1]);
    close(root_to_box[0]);

    double t_end = get_timestamp_sec();
    double total_wall_time = t_end - t_start;

    /* cálculo de tiempo real descontando los retardos de simulación */
    double real_time = total_wall_time;
    if (ttype == TASK_CASE_A_SIMULATION && sim_duration > 0.0 && !is_chaos) {
        int levels_waited = (final_root_msg.solver_level > 0) ? final_root_msg.solver_level : 1;
        real_time -= (levels_waited * sim_duration);
        if (real_time < 0.0001) real_time = 0.0001; /* margen computacional */
    }

    /* recopilación de estadísticas */
    sem_wait(&g_shm->mutex);
    bool task_completed = g_shm->task_solved && !is_chaos;
    int meeseeks_count = g_shm->meeseeks_count_current;
    int max_level = g_shm->max_level_reached;
    int s_pid = g_shm->solver_pid;
    int s_ppid = g_shm->solver_ppid;
    int s_lvl = g_shm->solver_level;
    int s_inst = g_shm->solver_instance;
    sem_post(&g_shm->mutex);

    /* registrar en historial */
    if (g_task_count < MAX_TASKS_HISTORY) {
        TaskRecord *rec = &g_history[g_task_count++];
        rec->task_id = g_task_count;
        rec->type = ttype;
        snprintf(rec->description, sizeof(rec->description), "%s", task_payload);
        rec->real_time_sec = real_time;
        rec->simulated_wait_sec = (ttype == TASK_CASE_A_SIMULATION) ? sim_duration : 0.0;
        rec->completed = task_completed;
        rec->planetary_chaos = is_chaos;
        rec->meeseeks_created = meeseeks_count;
        rec->max_level = max_level;
        rec->solver_pid = s_pid;
        rec->solver_ppid = s_ppid;
        rec->solver_level = s_lvl;
        rec->solver_instance = s_inst;
    }

    printf("[Box] Tarea concluida.\n"
           "  * Tiempo real de CPU/ejecución: %.4f s (tiempo total de pared: %.4f s)\n"
           "  * Mr. Meeseeks creados en esta tarea: %d (niveles: %d)\n"
           "  * Estado: %s\n\n",
           real_time, total_wall_time, meeseeks_count, max_level,
           task_completed ? "Completada con éxito" : (is_chaos ? "Fallida (caos planetario)" : "No completada"));
    fflush(stdout);
}

static void print_final_summary_report(void) {
    printf("\n----------------------------------------------------------------------------------------\n");
    printf("                    Informe final de actividad - Meeseeks Box                           \n");
    printf("----------------------------------------------------------------------------------------\n");
    printf("%-4s | %-7s | %-30s | %-10s | %-12s | %-8s\n",
           "ID", "Tipo", "Solicitud", "T. Real (s)", "Estado", "Meeseeks");
    printf("----------------------------------------------------------------------------------------\n");

    int completed_count = 0;
    int failed_count = 0;
    int total_meeseeks = 0;
    double total_real_time = 0.0;

    for (int i = 0; i < g_task_count; i++) {
        TaskRecord *r = &g_history[i];
        const char *t_str = (r->type == TASK_CASE_A_SIMULATION) ? "Caso A" :
                            (r->type == TASK_CASE_B_CALCULATION) ? "Caso B" : "Caso C";
        const char *s_str = r->completed ? "Resuelta" :
                            (r->planetary_chaos ? "Caos" : "No resuelta");

        printf("%-4d | %-7s | %-30.30s | %-10.4f | %-12s | %-8d\n",
               r->task_id, t_str, r->description, r->real_time_sec, s_str, r->meeseeks_created);

        if (r->completed) completed_count++;
        else failed_count++;
        total_meeseeks += r->meeseeks_created;
        total_real_time += r->real_time_sec;
    }

    printf("----------------------------------------------------------------------------------------\n");
    printf("Resumen global:\n");
    printf("  * Total de tareas solicitadas:         %d\n", g_task_count);
    printf("  * Tareas completadas exitosamente:    %d\n", completed_count);
    printf("  * Tareas no completadas / con fallo:  %d\n", failed_count);
    printf("  * Total de Mr. Meeseeks generados:    %d\n", total_meeseeks);
    printf("  * Tiempo real acumulado de ejecución: %.4f segundos\n", total_real_time);
    printf("----------------------------------------------------------------------------------------\n");
    printf("El programa Box se cierra. ¡Adiós!\n\n");
}

/* función principal main */

int main(int argc, char *argv[]) {
    srand((unsigned int)time(NULL));

    /* permitir configurar timeout de caos planetario por línea de comandos */
    if (argc >= 2) {
        int timeout_arg = atoi(argv[1]);
        if (timeout_arg > 0) {
            g_chaos_timeout_seconds = timeout_arg;
        }
    }

    setup_signals();

    if (!init_shared_memory()) {
        fprintf(stderr, "Error crítico: no se pudo inicializar la memoria compartida.\n");
        return EXIT_FAILURE;
    }

    print_box_banner();

    char line_buf[MAX_LINE_LEN];
    while (true) {
        printf("Box> Ingrese su solicitud: ");
        fflush(stdout);

        if (!fgets(line_buf, sizeof(line_buf), stdin)) {
            break; /* fin de entrada (EOF) */
        }

        /* eliminar salto de línea */
        line_buf[strcspn(line_buf, "\r\n")] = '\0';

        /* verificar comandos de despedida */
        if (strcasecmp(line_buf, "adios") == 0 ||
            strcasecmp(line_buf, "adiós") == 0 ||
            strcasecmp(line_buf, "salir") == 0 ||
            strcasecmp(line_buf, "exit") == 0 ||
            strcasecmp(line_buf, "bye") == 0) {
            printf("\n[Box] Recibida orden de despedida. Cerrando la caja...\n");
            break;
        }

        if (strlen(line_buf) > 0) {
            handle_user_request(line_buf);
        }
    }

    print_final_summary_report();
    cleanup_shared_memory();

    return EXIT_SUCCESS;
}
