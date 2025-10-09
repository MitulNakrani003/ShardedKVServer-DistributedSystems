
# 🧠 Distributed Systems Labs – MIT 6.824

### *“From Parallel Processing to Consensus – Building the Foundations of Distributed Computing.”*

---

## 🚀 Overview

This repository contains implementations of all the Labs from MIT’s **6.824: Distributed Systems** course.
Each lab progressively deepens understanding of building **fault-tolerant, parallel, and replicated systems** using the Go programming language.

---

## 🧩 Lab 1 – MapReduce

**Goal:** Build a simplified distributed MapReduce system that runs user-defined map and reduce tasks in parallel.

### ✳️ Highlights

* Implemented **Master–Worker coordination** via **Go RPC**, handling dynamic task allocation and worker crashes.
* Supported **fault recovery** by re-assigning tasks after timeout detection.
* Generated intermediate files using **JSON encoding** for deterministic reduce-phase aggregation.
* Achieved **100 % pass rate** on the parallelism, crash recovery, and correctness tests.

### 💡 Key Learnings

* Designing **distributed task scheduling** under failure conditions.
* Managing concurrency with **Go goroutines and synchronization primitives**.
* Applying atomic file operations (`os.Rename`) to ensure crash-safe writes.
* Gaining deep insight into the **MapReduce paper** through practical re-implementation.

---

## 🔁 Lab 2 – Raft Consensus Algorithm

**Goal:** Implement the **Raft** consensus protocol to maintain replicated logs and ensure consistent state across unreliable networks.

### ✳️ Highlights

* Built a **leader election**, **log replication**, and **persistence** mechanism across simulated servers.
* Implemented all three parts of the lab:

  * **2A:** Leader election and heartbeat mechanism.
  * **2B:** Log replication and follower consistency.
  * **2C:** State persistence and recovery after crash or reboot.
* Verified correctness with 100 % passing scores on all test suites (`2A`, `2B`, `2C`).
* Optimized election timeouts and RPC scheduling for deterministic recovery and efficient consensus.

### 💡 Key Learnings

* Developed an in-depth understanding of **distributed consensus** and **fault tolerance**.
* Learned how to maintain **replicated state machines** that remain consistent under partial failure.
* Practiced **lock management, concurrency control**, and **Go RPC message flow debugging**.
* Experienced real-world reliability engineering: heartbeat intervals, election backoffs, and log compaction design trade-offs.

---

## 🛠️ Tech Stack

* **Language:** Go (1.13+)
* **Concurrency:** goroutines, channels, mutexes, `sync.Cond`
* **Persistence:** Custom in-memory persister abstraction
* **RPC Framework:** Go net/rpc
* **Testing:** `go test`, custom scripts (`test-mr.sh`, `raft_test.go`)

---

## 📈 Impact

* Achieved **robust distributed computation and consensus simulation** entirely from scratch.
* Strengthened practical understanding of **scalable system design, recovery mechanisms, and deterministic computation**.
* Served as a foundation for future exploration into **sharded key/value stores**, **fault-tolerant services**, and **replicated databases**.


