# Airflow Single-Container Architecture (PoC Setup)

This document explains how the current Airflow setup works when running everything inside a single Docker container using manual commands:

```bash
airflow db init &&
airflow users create ... &&
airflow scheduler &
airflow webserver
```

This is a lightweight, development-oriented architecture suitable for Proof-of-Concept (PoC) work.

---

# 🔄 What Happens When Your DAG Runs

When you trigger a DAG, the following sequence occurs:

1. The **Scheduler** scans the DAG folder and detects a runnable task.
2. The Scheduler determines dependencies are satisfied.
3. The Scheduler launches a **task subprocess**.
4. That subprocess executes your operator (e.g., `SFTPOperator`).
5. The file is written to `/tmp` inside the container.
6. The Scheduler then launches the next task as another subprocess.
7. That subprocess reads the file from `/tmp`.
8. Logs are written to the container’s local filesystem.

Important: All of this happens inside the same Linux container and the same filesystem namespace.

---

# ✅ Why This Works Cleanly For You

Your setup works smoothly because:

* You are running a **single container**.
* The **Scheduler and Webserver** run inside that same container.
* The default executor (likely `SequentialExecutor`) runs tasks directly from the Scheduler.
* All tasks share the same container filesystem.
* `/tmp` is shared between all task subprocesses.

This means:

* Task 1 writes a file → Task 2 can read it.
* No distributed workers.
* No network file synchronization required.
* No shared storage configuration needed.

For a PoC, this simplicity is ideal.

---

# ⚠ Important Limitation of Your Setup

This architecture has several constraints:

### 1. No Parallelism

* `SequentialExecutor` runs one task at a time.
* No horizontal scaling.

### 2. SQLite Metadata Database

* Not production-grade.
* Limited concurrency support.

### 3. Ephemeral Local Storage

* Files written to `/tmp` exist only inside the container.
* If the container restarts → files are lost.
* If the container is deleted → everything is lost.

### 4. Single Point of Failure

* Scheduler, Webserver, DB, and tasks all run in one container.
* If it crashes → the entire system stops.

This is acceptable for development and PoC usage, but not for production workloads.

---

# 🏭 How This Would Differ in Real Deployment

In a production deployment, Airflow is typically split into multiple containers or services:

* Webserver container
* Scheduler container
* Worker containers
* External Postgres database
* (Optional) Redis for CeleryExecutor

In that setup:

* Tasks may run on different worker machines.
* Local disk is NOT shared between tasks.
* Writing to `/tmp` becomes unsafe.
* Durable shared storage (e.g., object storage) is required.

A typical production ELT pattern would look like:

```
SFTP → Cloud Storage → Data Warehouse
```

Local container storage is avoided entirely.

---

# 🧠 Clean Mental Model of YOUR Setup

Think of your environment as:

```
Mini All-In-One Airflow Node
```

Inside a single container you have:

* Scheduler process
* Webserver process
* SQLite metadata database
* Task subprocesses
* Log storage
* Local filesystem

Conceptually, it behaves like a small virtual machine running the entire Airflow stack.

Because everything lives inside one Linux environment:

* Process space is shared.
* Filesystem is shared.
* No distributed boundaries exist.

This makes it extremely simple and ideal for learning, testing connectivity (like SFTP), and demonstrating DAG structure.

---

# Summary

Your current setup is:

* Simple
* Self-contained
* Perfect for PoC
* Not production scalable

For early experimentation and architectural learning, this is exactly the right lev
