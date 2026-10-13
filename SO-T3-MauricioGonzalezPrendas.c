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
 *   Incorporación del evaluador analítico descendente recursivo para expresiones
 *   aritméticas y lógicas (caso B) y ejecución controlada de programas externos (caso C).
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
#include <ctype.h>
#include <errno.h>

#define MAX_LINE_LEN 1024

typedef enum {
    TASK_CASE_A_SIMULATION = 1,
    TASK_CASE_B_CALCULATION,
    TASK_CASE_C_EXTERNAL_PROG
} TaskType;

typedef struct {
    const char *expr;
    size_t pos;
    bool has_error;
    char error_msg[128];
} CalcParser;

static void skip_spaces(CalcParser *p) {
    while (p->expr[p->pos] && isspace((unsigned char)p->expr[p->pos])) p->pos++;
}

static double eval_expr_logical_or(CalcParser *p);

static double eval_primary(CalcParser *p) {
    skip_spaces(p);
    if (p->has_error) return 0.0;
    if (p->expr[p->pos] == '(') {
        p->pos++;
        double val = eval_expr_logical_or(p);
        skip_spaces(p);
        if (p->expr[p->pos] == ')') p->pos++;
        else {
            p->has_error = true;
            snprintf(p->error_msg, sizeof(p->error_msg), "Parentesis desbalanceado");
        }
        return val;
    }
    if (p->expr[p->pos] == '!' || p->expr[p->pos] == '-') {
        char op = p->expr[p->pos++];
        double val = eval_primary(p);
        if (op == '!') return (fabs(val) < 1e-9) ? 1.0 : 0.0;
        return -val;
    }
    char *endptr = NULL;
    double val = strtod(&p->expr[p->pos], &endptr);
    if (endptr == &p->expr[p->pos]) {
        p->has_error = true;
        snprintf(p->error_msg, sizeof(p->error_msg), "Numero o termino no valido");
        return 0.0;
    }
    p->pos = (size_t)(endptr - p->expr);
    return val;
}

static double eval_mul_div(CalcParser *p) {
    double left = eval_primary(p);
    while (!p->has_error) {
        skip_spaces(p);
        char c = p->expr[p->pos];
        if (c == '*' || c == '/' || c == '%') {
            p->pos++;
            double right = eval_primary(p);
            if (p->has_error) return 0.0;
            if (c == '*') left *= right;
            else if (c == '/') {
                if (fabs(right) < 1e-9) {
                    p->has_error = true;
                    snprintf(p->error_msg, sizeof(p->error_msg), "Division por cero detectada");
                    return 0.0;
                }
                left /= right;
            } else {
                if ((long)right == 0) {
                    p->has_error = true;
                    snprintf(p->error_msg, sizeof(p->error_msg), "Modulo por cero");
                    return 0.0;
                }
                left = (double)((long)left % (long)right);
            }
        } else break;
    }
    return left;
}

static double eval_add_sub(CalcParser *p) {
    double left = eval_mul_div(p);
    while (!p->has_error) {
        skip_spaces(p);
        char c = p->expr[p->pos];
        if (c == '+' || c == '-') {
            p->pos++;
            double right = eval_mul_div(p);
            if (p->has_error) return 0.0;
            if (c == '+') left += right;
            else left -= right;
        } else break;
    }
    return left;
}

static double eval_comparison(CalcParser *p) {
    double left = eval_add_sub(p);
    skip_spaces(p);
    if (p->has_error) return left;
    if (strncmp(&p->expr[p->pos], "==", 2) == 0) {
        p->pos += 2;
        return (fabs(left - eval_add_sub(p)) < 1e-9) ? 1.0 : 0.0;
    }
    if (strncmp(&p->expr[p->pos], "!=", 2) == 0) {
        p->pos += 2;
        return (fabs(left - eval_add_sub(p)) >= 1e-9) ? 1.0 : 0.0;
    }
    if (strncmp(&p->expr[p->pos], "<=", 2) == 0) {
        p->pos += 2;
        return (left <= eval_add_sub(p)) ? 1.0 : 0.0;
    }
    if (strncmp(&p->expr[p->pos], ">=", 2) == 0) {
        p->pos += 2;
        return (left >= eval_add_sub(p)) ? 1.0 : 0.0;
    }
    if (p->expr[p->pos] == '<') {
        p->pos++;
        return (left < eval_add_sub(p)) ? 1.0 : 0.0;
    }
    if (p->expr[p->pos] == '>') {
        p->pos++;
        return (left > eval_add_sub(p)) ? 1.0 : 0.0;
    }
    return left;
}

static double eval_expr_logical_and(CalcParser *p) {
    double left = eval_comparison(p);
    while (!p->has_error) {
        skip_spaces(p);
        if (strncmp(&p->expr[p->pos], "&&", 2) == 0) {
            p->pos += 2;
            double right = eval_comparison(p);
            left = ((fabs(left) > 1e-9) && (fabs(right) > 1e-9)) ? 1.0 : 0.0;
        } else break;
    }
    return left;
}

static double eval_expr_logical_or(CalcParser *p) {
    double left = eval_expr_logical_and(p);
    while (!p->has_error) {
        skip_spaces(p);
        if (strncmp(&p->expr[p->pos], "||", 2) == 0) {
            p->pos += 2;
            double right = eval_expr_logical_and(p);
            left = ((fabs(left) > 1e-9) || (fabs(right) > 1e-9)) ? 1.0 : 0.0;
        } else break;
    }
    return left;
}

static bool evaluate_arithmetic_logic(const char *expr, double *out_res, char *err_msg, size_t err_len) {
    CalcParser p;
    p.expr = expr;
    p.pos = 0;
    p.has_error = false;
    p.error_msg[0] = '\0';
    double res = eval_expr_logical_or(&p);
    skip_spaces(&p);
    if (!p.has_error && p.expr[p.pos] != '\0') {
        p.has_error = true;
        snprintf(p.error_msg, sizeof(p.error_msg), "Caracter inesperado al final");
    }
    if (p.has_error) {
        snprintf(err_msg, err_len, "%s", p.error_msg);
        return false;
    }
    *out_res = res;
    return true;
}

static bool execute_external_program(const char *cmd_line, int *out_exit_code) {
    pid_t pid = fork();
    if (pid < 0) return false;
    if (pid == 0) {
        execlp("/bin/sh", "sh", "-c", cmd_line, (char *)NULL);
        _exit(127);
    }
    int status;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        *out_exit_code = WEXITSTATUS(status);
        return (*out_exit_code == 0);
    }
    return false;
}

int main(void) {
    printf("----------------------------------------------------------------------------------------------------\n");
    printf("                                          BOX MR. MEESEEKS                                          \n");
    printf("----------------------------------------------------------------------------------------------------\n");
    printf("Bienvenido a la Box de Mr. Meeseeks.\n");
    printf("Comandos especiales:\n");
    printf("  'calc: <expresion>' -> Evalua operaciones aritmeticas y logicas (ej: calc: (5+3)*2 > 10)\n");
    printf("  'exec: <comando>'   -> Ejecuta un programa externo del sistema (ej: exec: ls -l)\n");
    printf("  'adios'             -> Cierra la Box\n");
    printf("----------------------------------------------------------------------------------------------------\n");

    char line[MAX_LINE_LEN];
    while (1) {
        printf("Box> Ingrese su solicitud: ");
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;
        line[strcspn(line, "\r\n")] = '\0';
        if (strlen(line) == 0) continue;
        if (strcmp(line, "adios") == 0 || strcmp(line, "salir") == 0) break;

        pid_t pid = fork();
        if (pid == 0) {
            printf("Hi, I'm Mr. Meeseeks! Look at Meeeee. (%d, %d, 1, 1)\n", getpid(), getppid());
            if (strncmp(line, "calc:", 5) == 0) {
                const char *expr = line + 5;
                while (*expr == ' ') expr++;
                printf("[Meeseeks %d]: Evaluando expresion: '%s'\n", getpid(), expr);
                double val = 0.0;
                char err[128];
                if (evaluate_arithmetic_logic(expr, &val, err, sizeof(err))) {
                    printf("[Meeseeks %d]: Resultado = %g%s\n", getpid(), val,
                           (val == 1.0) ? " (VERDADERO)" : (val == 0.0 ? " (FALSO)" : ""));
                } else {
                    printf("[Meeseeks %d]: [ERROR] %s.\n", getpid(), err);
                }
            } else if (strncmp(line, "exec:", 5) == 0) {
                const char *cmd = line + 5;
                while (*cmd == ' ') cmd++;
                printf("[Meeseeks %d]: Ejecutando comando del sistema: '%s'\n", getpid(), cmd);
                int code = 0;
                execute_external_program(cmd, &code);
                printf("[Meeseeks %d]: Programa finalizo con codigo de salida: %d%s\n",
                       getpid(), code, (code == 0) ? " (EXITO)" : " (FALLO)");
            }
            printf("[Meeseeks %d, nivel 1, instancia 1]: Tarea completada. Desapareciendo con un ¡POOF!\n", getpid());
            _exit(0);
        }
        waitpid(pid, NULL, 0);
    }
    printf("El programa Box se cierra. ¡Adiós!\n");
    return EXIT_SUCCESS;
}
