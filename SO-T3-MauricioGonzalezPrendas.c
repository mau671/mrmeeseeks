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
 *   Canales de comunicación privada mediante pares de tuberías bidireccionales
 *   y protocolo de mensajes MeeseeksMsg entre Box y los procesos subordinados.
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
#include <errno.h>

#define MAX_LINE_LEN 1024

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
    if (g_shm == MAP_FAILED) {
        perror("Error al mapear memoria compartida anónima");
        return false;
    }
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

static void run_meeseeks_process(int level, int instance, int read_from_parent_fd, int write_to_parent_fd) {
    shm_register_meeseeks_birth(level);
    printf("Hi, I'm Mr. Meeseeks! Look at Meeeee. (%d, %d, %d, %d)\n",
           getpid(), getppid(), level, instance);
    fflush(stdout);

    MeeseeksMsg assign_msg;
    memset(&assign_msg, 0, sizeof(assign_msg));
    ssize_t n = read(read_from_parent_fd, &assign_msg, sizeof(assign_msg));
    if (n <= 0) {
        close(read_from_parent_fd);
        close(write_to_parent_fd);
        _exit(1);
    }

    sleep(1);

    MeeseeksMsg resp;
    memset(&resp, 0, sizeof(resp));
    resp.type = MSG_TASK_SOLVED;
    resp.solver_pid = getpid();
    resp.solver_ppid = getppid();
    resp.solver_level = level;
    resp.solver_instance = instance;

    write(write_to_parent_fd, &resp, sizeof(resp));
    printf("[Meeseeks %d, nivel %d, instancia %d]: Tarea completada. Desapareciendo con un ¡POOF!\n",
           getpid(), level, instance);
    fflush(stdout);

    close(read_from_parent_fd);
    close(write_to_parent_fd);
    _exit(0);
}

int main(void) {
    if (!init_shared_memory()) {
        fprintf(stderr, "Fallo al inicializar memoria compartida.\n");
        return EXIT_FAILURE;
    }

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

        int box_to_root[2];
        int root_to_box[2];
        if (pipe(box_to_root) < 0 || pipe(root_to_box) < 0) {
            perror("Error al crear tuberías");
            continue;
        }

        pid_t pid = fork();
        if (pid < 0) {
            perror("Error en fork");
            close(box_to_root[0]); close(box_to_root[1]);
            close(root_to_box[0]); close(root_to_box[1]);
            continue;
        }

        if (pid == 0) {
            close(box_to_root[1]);
            close(root_to_box[0]);
            run_meeseeks_process(1, 1, box_to_root[0], root_to_box[1]);
        }

        close(box_to_root[0]);
        close(root_to_box[1]);

        MeeseeksMsg task_msg;
        memset(&task_msg, 0, sizeof(task_msg));
        task_msg.type = MSG_TASK_ASSIGN;
        task_msg.task_type = TASK_CASE_A_SIMULATION;
        snprintf(task_msg.text, sizeof(task_msg.text), "%s", line);
        write(box_to_root[1], &task_msg, sizeof(task_msg));

        MeeseeksMsg reply;
        memset(&reply, 0, sizeof(reply));
        read(root_to_box[0], &reply, sizeof(reply));
        waitpid(pid, NULL, 0);

        close(box_to_root[1]);
        close(root_to_box[0]);
        printf("[Box]: Solicitud procesada vía tuberías privadas.\n\n");
    }

    cleanup_shared_memory();
    printf("El programa Box se cierra. ¡Adiós!\n");
    return EXIT_SUCCESS;
}
