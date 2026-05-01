# 🚛 Fleet Assignment Optimization Pipeline

**PySpark • Docker • Airflow • Config-Driven Architecture**

---

## 🧠 Project Evolution (How this was built)

This project did **not start as a pipeline**.

It started as **exploration in notebooks**, and evolved into a production-style system.

### Phase 1 — Notebook-First Exploration

* Built initial logic in Jupyter notebooks
* Simulated:

  * ~100,000 trips
  * ~100 trucks
  * ~200 drivers
* Focus: **“Can we assign everything?”**

👉 Initial assumption: *more data = more assignments*

---

### Phase 2 — Reality Check (Key Insight)

Quickly realized:

```text id="k2x0qa"
Assignment is NOT a scaling problem
Assignment is a constraint problem
```

Even with large datasets:

```text id="6m2b6j"
100,000 trips ≠ 100,000 assignments
```

👉 Constraints like:

* driver availability
* rest time
* truck compatibility

made most assignments **theoretically impossible**

---

### Phase 3 — Feasibility-First Approach

Instead of forcing assignments:

```text id="l1hv5m"
Step 1: Identify feasible trips
Step 2: Assign only within feasible subset
```

Result:

```text id="qmwq6y"
100,000 trips
→ ~8,000 feasible
→ ~8,000 assigned
```

👉 This was the **core breakthrough**

---

### Phase 4 — Systemization

Converted notebook logic into:

```text id="q8y7wz"
✔ PySpark script
✔ Docker container
✔ Airflow DAG
✔ Config-driven system
```

---

## 🎯 Problem Statement

> Assign trips to drivers and trucks under real-world constraints while maximizing feasible assignments.

---

## ⚙️ Solution Overview

```text id="n6fhb3"
Airflow DAG
    ↓
DockerOperator
    ↓
Containerized PySpark Job
    ↓
Config + Data Driven Execution
    ↓
Constraint-Based Assignment
    ↓
Output (Delta / Parquet)
```

---

## 🏗️ Architecture

```text id="2x5w4p"
Airflow
   ↓
DockerOperator
   ↓
Fleet Job Container (PySpark)
   ↓
Data Layer (/app/data)
```

---

## 🛠️ Tech Stack

* PySpark
* Docker
* Apache Airflow
* YAML Config
* Delta / Parquet

---

## 📁 Repository Structure

```text id="v6j8q2"
fleet-optimization/
│
├── dags/
├── scripts/
├── config/
├── docker/
├── data/
├── notebooks/        ⭐ (exploration phase)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 📓 About Notebooks

Notebooks were used for:

```text id="j8p6ws"
✔ Initial data simulation
✔ Constraint modeling
✔ Rapid iteration of assignment logic
```

They are intentionally **kept separate from production code**.

👉 The system represents **validated logic**, not experimentation.

---

## 🔍 Key Engineering Decisions

### 1. Feasibility over Scale

* Reduced problem space before solving
* Avoided brute-force assignment

---

### 2. Config-Driven Design

* Centralized config.yaml
* Environment override via `DATA_PATH`

---

### 3. Immutable Execution

* Docker image contains:

  * code
  * config

👉 Only data is injected at runtime

---

### 4. Clean Separation

| Layer   | Responsibility |
| ------- | -------------- |
| Airflow | Orchestration  |
| Script  | Execution      |
| Config  | Behavior       |

---

## 🚧 Challenges Faced

### 🔴 Docker Volume Confusion

* Host vs container path mismatch
* Fixed using correct bind mounts + env variables

---

### 🔴 Airflow Execution Model

* Initially attempted docker-compose inside Airflow
* Learned correct usage of DockerOperator

---

### 🔴 Delta Lake Integration

* Spark failed to read delta format initially
* Fixed via Spark session configs

---

### 🔴 Dependency Management

* Large package installs failing
* Incorrect package names (yaml vs pyyaml)

---

## 📊 Results

```text id="9rxlkg"
Total Trips: 100,000
Feasible Trips: ~8,000
Assignments: ~8,000
```

👉 Demonstrates real-world constraint impact

---

## ▶️ How to Run

### Build

```bash id="d2x7yx"
docker compose build fleet_job
```

### Run locally

```bash id="8y2z0m"
docker run --rm \
  -v <DATA_PATH>:/app/data \
  -e DATA_PATH=/app/data \
  fleet-optimization-batch-fleet_job:latest
```

### Run via Airflow

```bash id="7c2g0g"
docker compose up airflow
```

---

## 🔮 Future Improvements

* Run Spark jobs via Kubernetes (Spark Operator)
* Integrate with cloud storage (S3 / Azure Data Lake)
* Lift-and-shift pipeline to Azure environment
* Add real-time trip assignment (streaming)
* Introduce cost optimization (not just feasibility)

---

## 🧠 What this project demonstrates

* Transition from **notebook experimentation → production pipeline**
* Understanding of **constraint-driven systems**
* Hands-on debugging of **containerized orchestration issues**
* Building **portable, scalable data pipelines**

---

## 💬 Final Note

This project is not about assigning trips.

It’s about understanding:

```text id="m5kz4v"
What CAN be assigned vs what CANNOT
```

👉 And designing systems accordingly.

---
