

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/*  Constants                                                           */
/* ------------------------------------------------------------------ */
#define MAX_PROCESSES   64
#define SIGTERM_CODE    15
#define SIGSTOP_CODE    19
#define SIGCONT_CODE    18

/* ------------------------------------------------------------------ */
/*  Process state                                                        */
/* ------------------------------------------------------------------ */
typedef enum {
    RUNNING,
    BLOCKED,
    STOPPED,      /* NEW: suspended via SIGSTOP */
    ZOMBIE,
    TERMINATED
} ProcessState;

/* ------------------------------------------------------------------ */
/*  Process Control Block                                                */
/* ------------------------------------------------------------------ */
typedef struct {
    int            pid;
    int            ppid;
    ProcessState   state;
    int            exit_status;

    /* Scheduling */
    int            priority;        /* 0 = highest, 3 = lowest         */

    /* Children list */
    int            children[MAX_PROCESSES];
    int            num_children;

    /* Per-process stats */
    unsigned long  cpu_ticks;       /* incremented each time the PCB   */
                                    /* participates in an operation     */
    struct timespec create_time;    /* wall-clock time of fork          */

    /* Synchronisation */
    pthread_cond_t cond;
} PCB;

/* ------------------------------------------------------------------ */
/*  Global state                                                         */
/* ------------------------------------------------------------------ */
PCB             process_table[MAX_PROCESSES];
int             next_pid      = 2;

pthread_mutex_t table_mutex;
pthread_cond_t  monitor_cv;
pthread_cond_t  worker_cv;
int             table_updated = 0;
int             all_done      = 0;
int             monitor_ready = 0;

__thread char   current_action[256];
char            global_action_msg[512];

FILE           *snapshots_file;

/* ---- Global statistics ---- */
int             stat_total_created   = 1;   /* init counts as 1        */
int             stat_peak_concurrent = 1;
int             stat_current_active  = 1;
int             stat_total_zombies   = 0;
int             stat_total_killed    = 0;

struct timespec prog_start_time;            /* set in main()            */

/* ------------------------------------------------------------------ */
/*  Helpers                                                              */
/* ------------------------------------------------------------------ */
static const char *state_to_string(ProcessState s)
{
    switch (s) {
        case RUNNING:    return "RUNNING";
        case BLOCKED:    return "BLOCKED";
        case STOPPED:    return "STOPPED";
        case ZOMBIE:     return "ZOMBIE";
        case TERMINATED: return "TERMINATED";
        default:         return "UNKNOWN";
    }
}

static int find_process(int pid)
{
    for (int i = 0; i < MAX_PROCESSES; i++)
        if (process_table[i].pid == pid &&
            process_table[i].state != TERMINATED)
            return i;
    return -1;
}

static int find_empty_slot(void)
{
    for (int i = 0; i < MAX_PROCESSES; i++)
        if (process_table[i].state == TERMINATED)
            return i;
    return -1;
}

/* Elapsed milliseconds since program start */
static long elapsed_ms(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (long)((now.tv_sec  - prog_start_time.tv_sec)  * 1000 +
                  (now.tv_nsec - prog_start_time.tv_nsec) / 1000000);
}

/* ------------------------------------------------------------------ */
/*  pm_ps  – snapshot (adds timestamp + priority columns)              */
/* ------------------------------------------------------------------ */
void pm_ps(FILE *f)
{
    fprintf(f, "[T+%ldms]\n", elapsed_ms());
    fprintf(f, "%-8s%-8s%-10s%-10s%-10s%-8s\n",
            "PID", "PPID", "STATE", "PRI", "CPU_TICKS", "EXIT");
    fprintf(f, "--------------------------------------------------\n");

    int active[MAX_PROCESSES];
    int count = 0;
    for (int i = 0; i < MAX_PROCESSES; i++)
        if (process_table[i].state != TERMINATED && process_table[i].pid != 0)
            active[count++] = i;

    /* Sort by priority first, then PID */
    for (int i = 1; i < count; i++) {
        int key = active[i];
        int j = i - 1;
        while (j >= 0 &&
               (process_table[active[j]].priority > process_table[key].priority ||
                (process_table[active[j]].priority == process_table[key].priority &&
                 process_table[active[j]].pid      >  process_table[key].pid))) {
            active[j + 1] = active[j];
            j--;
        }
        active[j + 1] = key;
    }

    for (int i = 0; i < count; i++) {
        int idx = active[i];
        fprintf(f, "%-8d%-8d%-10s%-10d%-10lu",
                process_table[idx].pid,
                process_table[idx].ppid,
                state_to_string(process_table[idx].state),
                process_table[idx].priority,
                process_table[idx].cpu_ticks);
        if (process_table[idx].state == ZOMBIE)
            fprintf(f, "%-8d\n", process_table[idx].exit_status);
        else
            fprintf(f, "-\n");
    }
    fprintf(f, "\n");
}

/* ------------------------------------------------------------------ */
/*  pm_stats  – summary printed at program end                         */
/* ------------------------------------------------------------------ */
void pm_stats(FILE *f)
{
    fprintf(f, "========================================\n");
    fprintf(f, "  PROCESS MANAGER — FINAL STATISTICS\n");
    fprintf(f, "========================================\n");
    fprintf(f, "  Total processes created  : %d\n", stat_total_created);
    fprintf(f, "  Peak concurrent          : %d\n", stat_peak_concurrent);
    fprintf(f, "  Total zombies reaped     : %d\n", stat_total_zombies);
    fprintf(f, "  Total killed (SIGTERM)   : %d\n", stat_total_killed);
    fprintf(f, "  Uptime                   : %ld ms\n", elapsed_ms());
    fprintf(f, "\n  Per-process CPU ticks:\n");

    /* Print stats for all slots that were ever used */
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].pid != 0)
            fprintf(f, "    PID %-4d  ticks=%-6lu  priority=%d\n",
                    process_table[i].pid,
                    process_table[i].cpu_ticks,
                    process_table[i].priority);
    }
    fprintf(f, "========================================\n");
}

/* ------------------------------------------------------------------ */
/*  signal_monitor                                                       */
/* ------------------------------------------------------------------ */
static void signal_monitor(const char *action_msg)
{
    if (action_msg && action_msg[0]) {
        strncpy(global_action_msg, action_msg, sizeof(global_action_msg) - 1);
        global_action_msg[sizeof(global_action_msg) - 1] = '\0';
    }
    table_updated = 1;
    pthread_cond_signal(&monitor_cv);
    while (table_updated)
        pthread_cond_wait(&worker_cv, &table_mutex);
}

/* ------------------------------------------------------------------ */
/*  pm_fork                                                              */
/* ------------------------------------------------------------------ */
void pm_fork(int parent_pid)
{
    pthread_mutex_lock(&table_mutex);

    int slot       = find_empty_slot();
    int parent_idx = find_process(parent_pid);

    if (slot != -1) {
        int new_pid = next_pid++;

        process_table[slot].pid          = new_pid;
        process_table[slot].ppid         = parent_pid;
        process_table[slot].state        = RUNNING;
        process_table[slot].exit_status  = 0;
        process_table[slot].num_children = 0;
        process_table[slot].cpu_ticks    = 0;
        /* Inherit parent priority, clamped to [0,3] */
        process_table[slot].priority     = (parent_idx != -1)
                                           ? process_table[parent_idx].priority
                                           : 1;
        memset(process_table[slot].children, 0,
               sizeof(process_table[slot].children));
        clock_gettime(CLOCK_MONOTONIC, &process_table[slot].create_time);

        if (parent_idx != -1) {
            process_table[parent_idx].cpu_ticks++;
            if (process_table[parent_idx].num_children < MAX_PROCESSES)
                process_table[parent_idx]
                    .children[process_table[parent_idx].num_children++] = new_pid;
        }

        /* Update statistics */
        stat_total_created++;
        stat_current_active++;
        if (stat_current_active > stat_peak_concurrent)
            stat_peak_concurrent = stat_current_active;

        signal_monitor(current_action);
        current_action[0] = '\0';
    }

    pthread_mutex_unlock(&table_mutex);
}

/* ------------------------------------------------------------------ */
/*  pm_exit                                                              */
/* ------------------------------------------------------------------ */
void pm_exit(int pid, int status)
{
    pthread_mutex_lock(&table_mutex);

    int idx = find_process(pid);
    if (idx != -1 &&
        process_table[idx].state != ZOMBIE &&
        process_table[idx].state != TERMINATED) {

        process_table[idx].state       = ZOMBIE;
        process_table[idx].exit_status = status;
        process_table[idx].cpu_ticks++;

        /* Orphan re-parenting: give children to PID 1 */
        for (int k = 0; k < process_table[idx].num_children; k++) {
            int cpid = process_table[idx].children[k];
            int cidx = find_process(cpid);
            if (cidx != -1) {
                process_table[cidx].ppid = 1;
                /* Register with init (slot 0) */
                int init_idx = find_process(1);
                if (init_idx != -1 &&
                    process_table[init_idx].num_children < MAX_PROCESSES)
                    process_table[init_idx]
                        .children[process_table[init_idx].num_children++] = cpid;
            }
        }

        stat_total_zombies++;

        int parent_idx = find_process(process_table[idx].ppid);
        if (parent_idx != -1)
            pthread_cond_broadcast(&process_table[parent_idx].cond);

        signal_monitor(current_action);
        current_action[0] = '\0';
    }

    pthread_mutex_unlock(&table_mutex);
}

/* ------------------------------------------------------------------ */
/*  pm_wait                                                              */
/* ------------------------------------------------------------------ */
void pm_wait(int parent_pid, int child_pid)
{
    pthread_mutex_lock(&table_mutex);

    int parent_idx = find_process(parent_pid);
    if (parent_idx == -1) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    int has_any_child = 0;
    for (int k = 0; k < process_table[parent_idx].num_children; k++) {
        if (find_process(process_table[parent_idx].children[k]) != -1) {
            has_any_child = 1;
            break;
        }
    }
    if (!has_any_child) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    char saved_action[256];
    strncpy(saved_action, current_action, sizeof(saved_action));
    saved_action[sizeof(saved_action) - 1] = '\0';
    current_action[0] = '\0';

    while (1) {
        int found_idx = -1;
        for (int k = 0; k < process_table[parent_idx].num_children; k++) {
            int cpid = process_table[parent_idx].children[k];
            int cidx = find_process(cpid);
            if (cidx != -1 && process_table[cidx].state == ZOMBIE) {
                if (child_pid == -1 || process_table[cidx].pid == child_pid) {
                    found_idx = cidx;
                    break;
                }
            }
        }

        if (found_idx != -1) {
            int reaped_pid = process_table[found_idx].pid;
            process_table[found_idx].state = TERMINATED;
            stat_current_active--;

            for (int k = 0; k < process_table[parent_idx].num_children; k++) {
                if (process_table[parent_idx].children[k] == reaped_pid) {
                    int last = --process_table[parent_idx].num_children;
                    process_table[parent_idx].children[k] =
                        process_table[parent_idx].children[last];
                    break;
                }
            }

            process_table[parent_idx].state = RUNNING;
            process_table[parent_idx].cpu_ticks++;
            signal_monitor(saved_action);

            pthread_mutex_unlock(&table_mutex);
            return;
        }

        if (process_table[parent_idx].state != BLOCKED) {
            process_table[parent_idx].state = BLOCKED;
            signal_monitor(saved_action);
            saved_action[0] = '\0';
        }

        pthread_cond_wait(&process_table[parent_idx].cond, &table_mutex);

        int still_has_child = 0;
        for (int k = 0; k < process_table[parent_idx].num_children; k++) {
            if (find_process(process_table[parent_idx].children[k]) != -1) {
                still_has_child = 1;
                break;
            }
        }
        if (!still_has_child) {
            process_table[parent_idx].state = RUNNING;
            pthread_mutex_unlock(&table_mutex);
            return;
        }
    }
}

/* ------------------------------------------------------------------ */
/*  pm_kill  – send SIGTERM to a single process                         */
/* ------------------------------------------------------------------ */
void pm_kill(int pid)
{
    pthread_mutex_lock(&table_mutex);

    int idx = find_process(pid);
    if (idx != -1 &&
        process_table[idx].state != ZOMBIE &&
        process_table[idx].state != TERMINATED) {

        process_table[idx].state       = ZOMBIE;
        process_table[idx].exit_status = SIGTERM_CODE;
        process_table[idx].cpu_ticks++;
        stat_total_killed++;

        int parent_idx = find_process(process_table[idx].ppid);
        if (parent_idx != -1)
            pthread_cond_broadcast(&process_table[parent_idx].cond);

        signal_monitor(current_action);
        current_action[0] = '\0';
    }

    pthread_mutex_unlock(&table_mutex);
}

/* ------------------------------------------------------------------ */
/*  pm_killall  — kill all children of parent_pid (NEW)                */
/* ------------------------------------------------------------------ */
void pm_killall(int parent_pid)
{
    pthread_mutex_lock(&table_mutex);

    int parent_idx = find_process(parent_pid);
    if (parent_idx == -1) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    int changed = 0;
    for (int k = 0; k < process_table[parent_idx].num_children; k++) {
        int cpid = process_table[parent_idx].children[k];
        int cidx = find_process(cpid);
        if (cidx != -1 &&
            process_table[cidx].state != ZOMBIE &&
            process_table[cidx].state != TERMINATED) {

            process_table[cidx].state       = ZOMBIE;
            process_table[cidx].exit_status = SIGTERM_CODE;
            process_table[cidx].cpu_ticks++;
            stat_total_killed++;
            changed = 1;
        }
    }
    pthread_cond_broadcast(&process_table[parent_idx].cond);

    if (changed) {
        signal_monitor(current_action);
        current_action[0] = '\0';
    }

    pthread_mutex_unlock(&table_mutex);
}

/* ------------------------------------------------------------------ */
/*  pm_signal  — SIGSTOP / SIGCONT / SIGTERM (NEW)                     */
/* ------------------------------------------------------------------ */
void pm_signal(int pid, int signo)
{
    pthread_mutex_lock(&table_mutex);

    int idx = find_process(pid);
    if (idx == -1) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    if (signo == SIGSTOP_CODE) {
        if (process_table[idx].state == RUNNING ||
            process_table[idx].state == BLOCKED) {
            process_table[idx].state = STOPPED;
            process_table[idx].cpu_ticks++;
            signal_monitor(current_action);
            current_action[0] = '\0';
        }
    } else if (signo == SIGCONT_CODE) {
        if (process_table[idx].state == STOPPED) {
            process_table[idx].state = RUNNING;
            process_table[idx].cpu_ticks++;
            signal_monitor(current_action);
            current_action[0] = '\0';
        }
    } else if (signo == SIGTERM_CODE) {
        if (process_table[idx].state != ZOMBIE &&
            process_table[idx].state != TERMINATED) {
            process_table[idx].state       = ZOMBIE;
            process_table[idx].exit_status = SIGTERM_CODE;
            process_table[idx].cpu_ticks++;
            stat_total_killed++;
            int parent_idx = find_process(process_table[idx].ppid);
            if (parent_idx != -1)
                pthread_cond_broadcast(&process_table[parent_idx].cond);
            signal_monitor(current_action);
            current_action[0] = '\0';
        }
    }

    pthread_mutex_unlock(&table_mutex);
}

/* ------------------------------------------------------------------ */
/*  pm_nice  — renice: adjust priority by delta, clamp to [0, 3]       */
/* ------------------------------------------------------------------ */
void pm_nice(int pid, int delta)
{
    pthread_mutex_lock(&table_mutex);

    int idx = find_process(pid);
    if (idx != -1) {
        int new_pri = process_table[idx].priority + delta;
        if (new_pri < 0) new_pri = 0;
        if (new_pri > 3) new_pri = 3;
        process_table[idx].priority = new_pri;
        process_table[idx].cpu_ticks++;
        signal_monitor(current_action);
        current_action[0] = '\0';
    }

    pthread_mutex_unlock(&table_mutex);
}

/* ------------------------------------------------------------------ */
/*  Monitor thread                                                       */
/* ------------------------------------------------------------------ */
void *monitor_thread(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&table_mutex);

    fprintf(snapshots_file, "Initial Process Table\n");
    pm_ps(snapshots_file);
    fflush(snapshots_file);

    monitor_ready = 1;
    pthread_cond_broadcast(&worker_cv);

    while (1) {
        while (!table_updated && !all_done)
            pthread_cond_wait(&monitor_cv, &table_mutex);

        if (all_done && !table_updated)
            break;

        if (global_action_msg[0]) {
            fprintf(snapshots_file, "%s", global_action_msg);
            global_action_msg[0] = '\0';
        }
        pm_ps(snapshots_file);
        fflush(snapshots_file);

        table_updated = 0;
        pthread_cond_broadcast(&worker_cv);
    }

    pthread_mutex_unlock(&table_mutex);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Worker thread                                                        */
/* ------------------------------------------------------------------ */
typedef struct {
    int   thread_id;
    char *filename;
} WorkerArgs;

void *worker_thread(void *arg)
{
    WorkerArgs *args = (WorkerArgs *)arg;
    FILE *f = fopen(args->filename, "r");
    if (!f) {
        printf("Could not open script file: %s\n", args->filename);
        free(args);
        return NULL;
    }

    char cmd[64];
    int  a1, a2;

    while (fscanf(f, "%63s", cmd) == 1) {

        if (strcmp(cmd, "fork") == 0) {
            (void)fscanf(f, "%d", &a1);
            snprintf(current_action, sizeof(current_action),
                     "Thread %d calls pm_fork(%d)\n", args->thread_id, a1);
            pm_fork(a1);

        } else if (strcmp(cmd, "exit") == 0) {
            (void)fscanf(f, "%d %d", &a1, &a2);
            snprintf(current_action, sizeof(current_action),
                     "Thread %d calls pm_exit(%d, %d)\n", args->thread_id, a1, a2);
            pm_exit(a1, a2);

        } else if (strcmp(cmd, "wait") == 0) {
            (void)fscanf(f, "%d %d", &a1, &a2);
            snprintf(current_action, sizeof(current_action),
                     "Thread %d calls pm_wait(%d, %d)\n", args->thread_id, a1, a2);
            pm_wait(a1, a2);

        } else if (strcmp(cmd, "kill") == 0) {
            (void)fscanf(f, "%d", &a1);
            snprintf(current_action, sizeof(current_action),
                     "Thread %d calls pm_kill(%d)\n", args->thread_id, a1);
            pm_kill(a1);

        } else if (strcmp(cmd, "killall") == 0) {
            (void)fscanf(f, "%d", &a1);
            snprintf(current_action, sizeof(current_action),
                     "Thread %d calls pm_killall(%d)\n", args->thread_id, a1);
            pm_killall(a1);

        } else if (strcmp(cmd, "signal") == 0) {
            (void)fscanf(f, "%d %d", &a1, &a2);
            snprintf(current_action, sizeof(current_action),
                     "Thread %d calls pm_signal(%d, %d)\n", args->thread_id, a1, a2);
            pm_signal(a1, a2);

        } else if (strcmp(cmd, "nice") == 0) {
            (void)fscanf(f, "%d %d", &a1, &a2);
            snprintf(current_action, sizeof(current_action),
                     "Thread %d calls pm_nice(%d, %d)\n", args->thread_id, a1, a2);
            pm_nice(a1, a2);

        } else if (strcmp(cmd, "sleep") == 0) {
            (void)fscanf(f, "%d", &a1);
            usleep((useconds_t)a1 * 1000);
        }
        /* Unknown commands are silently skipped */
    }

    fclose(f);
    free(args);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  main                                                                 */
/* ------------------------------------------------------------------ */
int main(int argc, char *argv[])
{
    if (argc < 2) {
        printf("Usage: %s <thread0.txt> [thread1.txt ...]\n", argv[0]);
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &prog_start_time);

    snapshots_file = fopen("snapshots.txt", "w");
    if (!snapshots_file) {
        printf("Error: cannot create snapshots.txt\n");
        return 1;
    }

    /* ---- Initialise sync primitives ---- */
    pthread_mutex_init(&table_mutex, NULL);
    pthread_cond_init(&monitor_cv,   NULL);
    pthread_cond_init(&worker_cv,    NULL);

    /* ---- Initialise process table ---- */
    memset(process_table, 0, sizeof(process_table));
    for (int i = 0; i < MAX_PROCESSES; i++) {
        process_table[i].state    = TERMINATED;
        process_table[i].pid      = 0;
        process_table[i].priority = 1;           /* default priority    */
        pthread_cond_init(&process_table[i].cond, NULL);
    }

    /* PID 1 — init process */
    process_table[0].pid          = 1;
    process_table[0].ppid         = 0;
    process_table[0].state        = RUNNING;
    process_table[0].exit_status  = 0;
    process_table[0].priority     = 0;           /* init has top priority */
    process_table[0].num_children = 0;
    process_table[0].cpu_ticks    = 0;
    clock_gettime(CLOCK_MONOTONIC, &process_table[0].create_time);

    /* ---- Start monitor thread ---- */
    pthread_t monitor_tid;
    pthread_create(&monitor_tid, NULL, monitor_thread, NULL);

    pthread_mutex_lock(&table_mutex);
    while (!monitor_ready)
        pthread_cond_wait(&worker_cv, &table_mutex);
    pthread_mutex_unlock(&table_mutex);

    /* ---- Start worker threads ---- */
    int num_workers = argc - 1;
    pthread_t *workers = malloc(sizeof(pthread_t) * (size_t)num_workers);

    for (int i = 0; i < num_workers; i++) {
        WorkerArgs *args = malloc(sizeof(WorkerArgs));
        args->thread_id = i;
        args->filename  = argv[i + 1];
        pthread_create(&workers[i], NULL, worker_thread, args);
    }

    for (int i = 0; i < num_workers; i++)
        pthread_join(workers[i], NULL);

    /* ---- Shut down monitor ---- */
    pthread_mutex_lock(&table_mutex);
    all_done = 1;
    pthread_cond_broadcast(&monitor_cv);
    pthread_mutex_unlock(&table_mutex);

    pthread_join(monitor_tid, NULL);

    /* ---- Final statistics ---- */
    pm_stats(snapshots_file);

    /* ---- Cleanup ---- */
    fclose(snapshots_file);

    for (int i = 0; i < MAX_PROCESSES; i++)
        pthread_cond_destroy(&process_table[i].cond);

    pthread_mutex_destroy(&table_mutex);
    pthread_cond_destroy(&monitor_cv);
    pthread_cond_destroy(&worker_cv);
    free(workers);

    return 0;
}
