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
 *   Salvaguarda de contingencia finita para dificultad cero, aislamiento mediante
 *   grupos de procesos con setpgid y temporizador de alarma para caos planetario.
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

#define MAX_LINE_LEN 1024
#define DEFAULT_CHAOS_TIMEOUT 300
#define CONTINGENCY_MAX_LEVEL 3

static volatile sig_atomic_t g_chaos_occurred = 0;
static int g_chaos_timeout_seconds = DEFAULT_CHAOS_TIMEOUT;

static void sigalrm_handler(int sig) {
    (void)sig;
    g_chaos_occurred = 1;
}

static void setup_signals(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigalrm_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGALRM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
}

int main(void) {
    setup_signals();
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

        if (strncmp(line, "config: timeout ", 16) == 0) {
            int val = atoi(line + 16);
            if (val > 0) {
                g_chaos_timeout_seconds = val;
                printf("[Box]: Tiempo limite para simulaciones configurado a %d segundos.\n", val);
            }
            continue;
        }

        pid_t root_pid = fork();
        if (root_pid == 0) {
            setpgid(0, 0);
            printf("Hi, I'm Mr. Meeseeks! Look at Meeeee. (%d, %d, 1, 1)\n", getpid(), getppid());
            sleep(g_chaos_timeout_seconds + 1);
            _exit(0);
        }

        g_chaos_occurred = 0;
        alarm((unsigned int)g_chaos_timeout_seconds);

        while (1) {
            int status;
            pid_t wp = waitpid(root_pid, &status, WNOHANG);
            if (wp == root_pid) break;
            if (g_chaos_occurred) {
                printf("\n--------------------------------------------------------------------------------\n");
                printf("¡ALERTA DE CAOS PLANETARIO!\n");
                printf("Los Meeseeks han existido durante demasiado tiempo (limite de %d segundos superado).\n", g_chaos_timeout_seconds);
                printf("Terminando todos los procesos Meeseeks para prevenir el colapso del sistema...\n");
                printf("--------------------------------------------------------------------------------\n");
                killpg(root_pid, SIGTERM);
                usleep(200000);
                killpg(root_pid, SIGKILL);
                waitpid(root_pid, &status, 0);
                break;
            }
            usleep(20000);
        }
        alarm(0);
    }
    printf("El programa Box se cierra. ¡Adiós!\n");
    return EXIT_SUCCESS;
}
