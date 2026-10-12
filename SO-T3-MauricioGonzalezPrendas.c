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
 *   Simulación probabilística de consultas textuales con dilución de dificultad
 *   por envejecimiento y bifurcación jerárquica en abanico de 2 o 5 Meeseeks hijos.
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
#include <math.h>
#include <errno.h>

#define MAX_LINE_LEN 1024
#define MAX_CHILDREN_NODES 32

typedef enum {
    TASK_CASE_A_SIMULATION = 1,
    TASK_CASE_B_CALCULATION,
    TASK_CASE_C_EXTERNAL_PROG
} TaskType;

typedef enum {
    MSG_TASK_ASSIGN = 1,
    MSG_TASK_SOLVED,
    MSG_TASK_FAILED,
    MSG_TERMINATE_ORDER,
    MSG_FAREWELL_ACK
} PipeMsgType;

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

typedef struct {
    int to_child[2];
    int to_parent[2];
    pid_t pid;
    int instance;
} ChildPipe;

typedef struct {
    sem_t mutex;
    int meeseeks_count_current;
    int meeseeks_count_total;
    int max_level_reached;
    int solver_pid;
    int solver_ppid;
    int solver_level;
    int solver_instance;
    bool task_solved;
} SharedContext;

static SharedContext *g_shm = NULL;

static bool init_shared_memory(void) {
    g_shm = (SharedContext *)mmap(NULL, sizeof(SharedContext),
                                  PROT_READ | PROT_WRITE,
                                  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (g_shm == MAP_FAILED) return false;
    if (sem_init(&g_shm->mutex, 1, 1) != 0) {
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

static void cleanup_shared_memory(void) {
    if (g_shm != NULL) {
        sem_destroy(&g_shm->mutex);
        munmap(g_shm, sizeof(SharedContext));
        g_shm = NULL;
    }
}

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

static double generate_random_difficulty(void) {
    double u1 = (double)rand() / (double)RAND_MAX;
    double u2 = (double)rand() / (double)RAND_MAX;
    if (u1 <= 0.0001) u1 = 0.0001;
    double z = sqrt(-2.0 * log(u1)) * cos(2.0 * M_PI * u2);
    double val = 50.0 + z * 20.0;
    if (val < 0.0) val = 0.0;
    if (val > 100.0) val = 100.0;
    return val;
}

static void run_meeseeks_process(int level, int instance, int read_from_parent_fd, int write_to_parent_fd);

static void run_meeseeks_process(int level, int instance, int read_from_parent_fd, int write_to_parent_fd) {
    shm_register_meeseeks_birth(level);
    printf("Hi, I'm Mr. Meeseeks! Look at Meeeee. (%d, %d, %d, %d)\n",
           getpid(), getppid(), level, instance);
    fflush(stdout);

    MeeseeksMsg in_msg;
    memset(&in_msg, 0, sizeof(in_msg));
    if (read(read_from_parent_fd, &in_msg, sizeof(in_msg)) <= 0) {
        close(read_from_parent_fd);
        close(write_to_parent_fd);
        _exit(1);
    }

    double diff = in_msg.final_difficulty;
    printf("[Meeseeks %d, nivel %d, instancia %d] Iniciando simulacion (dificultad: %.2f, tiempo: %.2f s)...\n",
           getpid(), level, instance, diff, in_msg.sim_duration);
    fflush(stdout);

    usleep((useconds_t)(in_msg.sim_duration * 1000000.0));

    if (diff > 85.0) {
        printf("[Meeseeks %d, nivel %d, instancia %d]: Tarea resuelta exitosamente.\n",
               getpid(), level, instance);
        sem_wait(&g_shm->mutex);
        g_shm->task_solved = true;
        g_shm->solver_pid = getpid();
        g_shm->solver_ppid = getppid();
        g_shm->solver_level = level;
        g_shm->solver_instance = instance;
        sem_post(&g_shm->mutex);

        MeeseeksMsg resp;
        memset(&resp, 0, sizeof(resp));
        resp.type = MSG_TASK_SOLVED;
        resp.solver_pid = getpid();
        resp.solver_level = level;
        resp.solver_instance = instance;
        write(write_to_parent_fd, &resp, sizeof(resp));

        printf("[Meeseeks %d, nivel %d, instancia %d]: Tarea completada. Desapareciendo con un ¡POOF!\n",
               getpid(), level, instance);
        close(read_from_parent_fd);
        close(write_to_parent_fd);
        _exit(0);
    }

    int children_count = (diff <= 45.0) ? 5 : 2;
    printf("[Meeseeks %d, nivel %d, instancia %d]: Dificultad %.2f en rango %s. Creando %d Meeseeks subordinados...\n",
           getpid(), level, instance, diff, (children_count == 5) ? "[0, 45]" : "(45, 85]", children_count);
    fflush(stdout);

    ChildPipe children[MAX_CHILDREN_NODES];
    for (int i = 0; i < children_count; i++) {
        pipe(children[i].to_child);
        pipe(children[i].to_parent);
        children[i].instance = i + 1;

        pid_t cpid = fork();
        if (cpid == 0) {
            close(read_from_parent_fd);
            close(write_to_parent_fd);
            close(children[i].to_child[1]);
            close(children[i].to_parent[0]);
            for (int k = 0; k < i; k++) {
                close(children[k].to_child[1]);
                close(children[k].to_parent[0]);
            }
            run_meeseeks_process(level + 1, i + 1, children[i].to_child[0], children[i].to_parent[1]);
        }
        children[i].pid = cpid;
        close(children[i].to_child[0]);
        close(children[i].to_parent[1]);

        MeeseeksMsg cmsg = in_msg;
        double aged_diff = (diff == 0.0) ? 0.0 : diff + (100.0 - diff) * 0.5;
        if (aged_diff > 100.0) aged_diff = 100.0;
        cmsg.final_difficulty = aged_diff;
        write(children[i].to_child[1], &cmsg, sizeof(cmsg));
    }

    for (int i = 0; i < children_count; i++) {
        waitpid(children[i].pid, NULL, 0);
        close(children[i].to_child[1]);
        close(children[i].to_parent[0]);
    }

    MeeseeksMsg parent_resp;
    memset(&parent_resp, 0, sizeof(parent_resp));
    parent_resp.type = MSG_TASK_SOLVED;
    write(write_to_parent_fd, &parent_resp, sizeof(parent_resp));

    printf("[Meeseeks %d, nivel %d, instancia %d]: Tarea completada. Desapareciendo con un ¡POOF!\n",
           getpid(), level, instance);
    close(read_from_parent_fd);
    close(write_to_parent_fd);
    _exit(0);
}

int main(void) {
    srand((unsigned int)time(NULL));
    if (!init_shared_memory()) return EXIT_FAILURE;

    printf("----------------------------------------------------------------------------------------------------\n");
    printf("                                          BOX MR. MEESEEKS                                          \n");
    printf("----------------------------------------------------------------------------------------------------\n");
    printf("Bienvenido a la Box de Mr. Meeseeks.\n");

    char line[MAX_LINE_LEN];
    while (1) {
        printf("Box> Ingrese su solicitud: ");
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;
        line[strcspn(line, "\r\n")] = '\0';
        if (strlen(line) == 0) continue;
        if (strcmp(line, "adios") == 0 || strcmp(line, "salir") == 0) break;

        printf("Como desea determinar la dificultad de la solicitud?\n");
        printf("  1. Ingresar dificultad manualmente (0 a 100)\n");
        printf("  2. Dejarlo a la suerte de Mr. Meeseeks (distribucion aleatoria)\n");
        printf("Seleccione opcion (1 o 2) [Enter para 2]: ");
        fflush(stdout);

        char opt[32];
        double diff = 50.0;
        if (fgets(opt, sizeof(opt), stdin) && (opt[0] == '1')) {
            printf("Ingrese la dificultad (0 = imposible, 100 = trivial): ");
            fflush(stdout);
            char dstr[64];
            if (fgets(dstr, sizeof(dstr), stdin)) diff = atof(dstr);
        } else {
            diff = generate_random_difficulty();
        }

        int b2r[2], r2b[2];
        pipe(b2r); pipe(r2b);

        pid_t pid = fork();
        if (pid == 0) {
            close(b2r[1]); close(r2b[0]);
            run_meeseeks_process(1, 1, b2r[0], r2b[1]);
        }
        close(b2r[0]); close(r2b[1]);

        MeeseeksMsg msg;
        memset(&msg, 0, sizeof(msg));
        msg.type = MSG_TASK_ASSIGN;
        msg.task_type = TASK_CASE_A_SIMULATION;
        msg.final_difficulty = diff;
        msg.sim_duration = 1.0;
        snprintf(msg.text, sizeof(msg.text), "%s", line);
        write(b2r[1], &msg, sizeof(msg));

        MeeseeksMsg reply;
        read(r2b[0], &reply, sizeof(reply));
        waitpid(pid, NULL, 0);

        close(b2r[1]); close(r2b[0]);
        printf("[Box]: ¡Solicitud completada con exito!\n\n");
    }

    cleanup_shared_memory();
    printf("El programa Box se cierra. ¡Adiós!\n");
    return EXIT_SUCCESS;
}
