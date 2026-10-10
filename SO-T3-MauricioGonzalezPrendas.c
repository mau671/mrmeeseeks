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
 *   Estructura base de Box Mr. Meeseeks con memoria compartida anónima,
 *   semáforos POSIX y bifurcación inicial del proceso raíz.
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

        pid_t pid = fork();
        if (pid < 0) {
            perror("Error al crear Mr. Meeseeks");
            continue;
        }
        if (pid == 0) {
            shm_register_meeseeks_birth(1);
            printf("Hi, I'm Mr. Meeseeks! Look at Meeeee. (%d, %d, 1, 1)\n", getpid(), getppid());
            sleep(1);
            printf("[Meeseeks %d, nivel 1, instancia 1]: Tarea completada. Desapareciendo con un ¡POOF!\n", getpid());
            _exit(0);
        }
        waitpid(pid, NULL, 0);
    }

    cleanup_shared_memory();
    printf("El programa Box se cierra. ¡Adiós!\n");
    return EXIT_SUCCESS;
}
