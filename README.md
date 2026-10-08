# Multi-Threaded Producer-Consumer System in C

![C](https://img.shields.io/badge/Language-C-00599C?style=flat&logo=c&logoColor=white)
![POSIX Threads](https://img.shields.io/badge/Concurrency-pthreads%20%26%20semaphores-informational?style=flat)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## 📄 Overview
A multi-threaded concurrent simulation system implemented in **C** using **POSIX threads (`pthreads`)** and **semaphores (`sem_t`)**. The application implements a classic **Producer-Consumer pattern** using a dynamic circular buffer and thread synchronization mechanisms to manage data exchange between suppliers, clients, and an invoicing engine without race conditions or deadlocks.

## 🏗️ System Architecture & Workflow

1. **Provider Threads (Producers):** Read incoming binary/text data files for $P$ suppliers, validate product types ('a' through 'j'), and insert structured items into a shared circular buffer[cite: 10].
2. **Consumer Threads (Consumers):** Concurrently read items from the circular buffer, process individual client requests, and append summary metrics to a thread-safe linked list[cite: 10].
3. **Biller Thread (Invoicing Engine):** Traverses the shared linked list upon completion of supplier processing to generate global output statistics, per-provider metrics, and identify top consumers[cite: 10].

```text
[ Provider Threads ] ---> ( Circular Buffer ) ---> [ Consumer Threads ] ---> [ Linked List ] ---> [ Biller Thread ]
```

## 🔐 Synchronization & Concurrency Control
- **Mutex Semaphores (`mutex_esc_buffer`, `mutex_lee_buffer`):** Ensure mutually exclusive access during circular buffer writes and reads[cite: 10].
- **Counting Semaphores (`hay_espacio`, `hay_dato_buf_o_fin`):** Regulate buffer limits, preventing overflow/underflow conditions[cite: 10].
- **File & List Mutexes (`mutex_fich`, `mutex_esc_lista`):** Prevent race conditions during concurrent output file writes and dynamic node insertions into the linked list[cite: 10].

## 🚀 How to Build and Run

### Prerequisites
- GCC Compiler with POSIX Threads library (`-pthread`)[cite: 10]
- Make utility

### Compilation
```bash
make
```

### Execution
Run the compiled binary passing system parameters:
```bash
./main <input_path> <output_file> <buffer_size> <num_providers> <num_consumers>
```

**Parameters:**
- `<input_path>`: Directory path containing provider data files[cite: 10].
- `<output_file>`: Target file path for generated statistics[cite: 10].
- `<buffer_size>`: Circular buffer capacity $T$ ($1 \le T \le 5000$)[cite: 10].
- `<num_providers>`: Number of provider threads $P$ ($1 \le P \le 7$)[cite: 10].
- `<num_consumers>`: Number of consumer threads $C$ ($1 \le C \le 1000$)[cite: 10].

## 👥 Authors
- **Javier Vaca Sánchez** ([@javivsr](https://github.com/javivsr))[cite: 10]
- **Enrique Quiñones Ureta**[cite: 10]