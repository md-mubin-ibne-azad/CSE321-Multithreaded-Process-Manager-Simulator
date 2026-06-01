# CSE321-Multithreaded-Process-Manager-Simulator

A systems-level simulation of a Unix-style process manager written in **C with POSIX threads**. Simulates the core of an OS kernel's process subsystem — including process lifecycle, inter-process synchronisation, signal delivery, priority scheduling, and real-time diagnostics — all driven by a concurrent multi-worker scripting engine.

> Built for CSE 321 (Operating Systems) and extended with production-grade features for a portfolio-quality systems project.

---

## 🧠 What This Demonstrates

This project directly mirrors concepts used in production operating systems and systems software:

| Concept | Where It Appears |
|---------|-----------------|
| Process Control Blocks (PCB) | `struct PCB` with full lifecycle fields |
| Mutex + Condition Variable synchronisation | `pthread_mutex_t` / `pthread_cond_t` throughout |
| Monitor pattern | Dedicated `monitor_thread` with handshake protocol |
| Fork / Exit / Wait / Kill | `pm_fork`, `pm_exit`, `pm_wait`, `pm_kill` |
| Zombie reaping | Parent uses `pm_wait` to reap ZOMBIE children |
| Orphan re-parenting | Children re-parented to PID 1 (init) on parent exit |
| UNIX signal simulation | `pm_signal` with SIGSTOP / SIGCONT / SIGTERM |
| Priority scheduling | Per-process `priority` field; snapshots sorted by priority |
| `nice` / renice | `pm_nice(pid, delta)` adjusts priority at runtime |
| CPU tick accounting | Per-PCB `cpu_ticks` counter tracks operation participation |
| Wall-clock timestamps | `CLOCK_MONOTONIC` timestamps on every snapshot |
| Concurrent script workers | One `pthread` per script file, all sharing one process table |

---

## 🆕 Enhanced Features (over original submission)

| Feature | Description |
|---------|-------------|
| **`STOPPED` state** | New process state for suspended processes |
| **`pm_signal(pid, signo)`** | Delivers SIGSTOP (19), SIGCONT (18), or SIGTERM (15) |
| **`pm_nice(pid, delta)`** | Renices a process; clamped to priority range [0, 3] |
| **`pm_killall(ppid)`** | Atomically kills all children of a given parent |
| **Priority-sorted snapshots** | Process table sorted by priority then PID |
| **CPU tick counters** | Each PCB tracks how many operations it participated in |
| **Orphan re-parenting** | Children of an exiting process are adopted by PID 1 |
| **Wall-clock timestamps** | Every snapshot prefixed with `[T+Xms]` |
| **`pm_stats()` summary** | Final report: total created, peak concurrency, uptime |
| **Priority inheritance on fork** | Child inherits parent's priority at creation |
| **`signal` / `nice` script commands** | New commands for thread script files |

---

## 🏗️ Architecture

```
main()
 ├─ Initialises process table (PID 1 = init, priority 0)
 ├─ Spawns monitor_thread  ──────────────────────────────┐
 └─ Spawns N worker_threads (one per script file)        │
      │                                                   │
      │  Each worker reads commands, calls pm_*()         │
      │  pm_*() locks table_mutex, mutates PCBs,          │
      │  then calls signal_monitor() which:               │
      │    → sets table_updated = 1                       │
      │    → signals monitor_cv   ──────────────────────► │
      │    → waits on worker_cv   ◄────────────────────── │ (monitor acks)
      │                                                   │
      └─────────────────────────────────────────────────── monitor prints
                                                           snapshot + flushes
```

The **monitor pattern** ensures snapshots are serialised and complete — no torn reads, no out-of-order output, even under high concurrency.

---

## 📁 Project Structure

```
Process_Manager_Simulator/
│
├── pm_sim_enhanced.c      ← Main source (enhanced edition)
├── pm_sim.c               ← Original submission
├── thread0.txt            ← Example worker script
├── thread1.txt
├── thread2.txt
├── thread3.txt
└── snapshots.txt          ← Generated output (process table history)
```

---

## 🔧 Build & Run

### Compile

```bash
gcc -O2 -pthread pm_sim_enhanced.c -o pm_sim
```

### Run

```bash
./pm_sim thread0.txt thread1.txt thread2.txt thread3.txt
```

Each `.txt` file is a worker script. Any number of files can be passed.

### View output

```bash
cat snapshots.txt
```

---

## 📜 Script Command Reference

Each worker script file contains one command per line:

| Command | Arguments | Action |
|---------|-----------|--------|
| `fork` | `<ppid>` | Create a new child process under parent `ppid` |
| `exit` | `<pid> <status>` | Transition `pid` to ZOMBIE with exit code `status` |
| `wait` | `<ppid> <child_pid>` | Block parent until child exits; -1 = any child |
| `kill` | `<pid>` | Send SIGTERM → transitions `pid` to ZOMBIE (exit 15) |
| `killall` | `<ppid>` | Kill all children of `ppid` atomically |
| `signal` | `<pid> <signo>` | Deliver signal: 19=SIGSTOP, 18=SIGCONT, 15=SIGTERM |
| `nice` | `<pid> <delta>` | Adjust `pid` priority by delta (clamped to 0–3) |
| `sleep` | `<ms>` | Pause this worker thread for N milliseconds |

### Example script
```
fork 1
sleep 100
nice 2 1
signal 3 19
sleep 200
signal 3 18
wait 1 -1
killall 1
```

---

## 📊 Sample Output

```
Initial Process Table
[T+7ms]
PID     PPID    STATE     PRI       CPU_TICKS EXIT
--------------------------------------------------
1       0       RUNNING   0         0         -

Thread 0 calls pm_fork(1)
[T+7ms]
PID     PPID    STATE     PRI       CPU_TICKS EXIT
--------------------------------------------------
1       0       RUNNING   0         1         -
2       1       RUNNING   0         0         -

Thread 1 calls pm_signal(3, 19)
[T+158ms]
PID     PPID    STATE     PRI       CPU_TICKS EXIT
--------------------------------------------------
1       0       RUNNING   0         2         -
3       1       STOPPED   0         1         -     ← SIGSTOP applied
2       1       RUNNING   1         1         -

========================================
  PROCESS MANAGER — FINAL STATISTICS
========================================
  Total processes created  : 4
  Peak concurrent          : 3
  Total zombies reaped     : 1
  Total killed (SIGTERM)   : 2
  Uptime                   : 609 ms

  Per-process CPU ticks:
    PID 1     ticks=4       priority=0
    PID 2     ticks=2       priority=1
    PID 4     ticks=1       priority=0
========================================
```

---

## 🔒 Synchronisation Design

| Primitive | Purpose |
|-----------|---------|
| `pthread_mutex_t table_mutex` | Single global lock protecting the entire process table |
| `pthread_cond_t monitor_cv` | Workers signal this to wake the monitor |
| `pthread_cond_t worker_cv` | Monitor signals this to unblock workers after a snapshot |
| `pthread_cond_t PCB.cond` | Per-process cond var; parent sleeps here waiting for child exit |

The monitor uses a **rendezvous / handshake** protocol: a worker mutates the table, signals the monitor, then **blocks** until the monitor acknowledges. This guarantees every state change appears in exactly one atomic snapshot — no missed transitions, no double-printing.

---

## 🚀 Concepts Covered

- **POSIX Threads (pthreads):** thread creation, joining, mutex locking, condition variables
- **Process lifecycle:** fork → running → blocked/stopped → zombie → terminated
- **Zombie reaping:** parent must call `wait` to release a zombie's resources
- **Orphan re-parenting:** mirrors Linux `init`/`systemd` adopting orphaned processes
- **Signal delivery model:** simplified Unix signal semantics (SIGSTOP / SIGCONT / SIGTERM)
- **Priority scheduling:** processes ranked 0–3; scheduler always picks lowest-number priority first
- **Monitor design pattern:** concurrent-safe observer for shared state

---

## 🛠️ Future Extensions

- **Round-robin CPU scheduler** with configurable time quanta
- **Pipe / IPC simulation** between processes
- **Memory allocation tracker** per PCB (virtual memory simulation)
- **REST API / socket interface** to control the simulator remotely
- **ncurses live dashboard** showing the process table in real time

---

## 📚 References

- *Operating System Concepts* — Silberschatz, Galvin, Gagne (10th ed.)
- `man 7 pthreads` — POSIX Threads manual
- `man 2 waitpid`, `man 2 kill`, `man 2 nice` — Linux syscall references
- *The Linux Programming Interface* — Michael Kerrisk
