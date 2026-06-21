# Apache Airflow — Learning Project

> Part of the Data Engineering Tools series. Each tool gets its own containerized project and learning path.

Apache Airflow is the industry-standard workflow orchestration platform. You define pipelines as code (Python), schedule them, monitor them, and wire together data systems through a library of hundreds of integrations called **Providers**.

---

## Stack

| Component | Version |
|-----------|---------|
| Apache Airflow | 3.2.2 |
| Executor | LocalExecutor (single-node, ideal for learning) |
| Metadata DB | PostgreSQL 16 |
| Python | 3.11+ |

---

## Quick Start

```bash
# 1. Start all services (first run initializes the DB and creates the admin user)
docker compose up airflow-init
docker compose up -d

# 2. Open the UI
open http://localhost:8080
# Login: airflow / airflow

# 3. Tail logs
docker compose logs -f airflow-scheduler

# 4. Stop everything
docker compose down

# 5. Stop and wipe the database (full reset)
docker compose down -v
```

### Local Python Environment (for IDE support & testing)

```bash
# Using uv (recommended)
uv venv && uv pip install -e ".[dev]"

# Or pip
python -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"
```

### Running a Bash command inside Airflow

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow tasks test <dag_id> <task_id> <logical_date>
```

---

## Project Structure

```
01-apache-airflow/
├── dags/              ← your DAG files go here (auto-detected)
├── plugins/           ← custom operators, hooks, sensors
├── logs/              ← task logs (git-ignored)
├── config/            ← airflow.cfg overrides
├── tests/             ← DAG unit tests
├── docker-compose.yml
├── pyproject.toml
└── .env               ← local env vars (git-ignored)
```

---

## Learning Path

Work through each milestone in order. Each one builds on the previous. Create a DAG file in `dags/` for each milestone as hands-on practice.

---

### 1. Getting Started

- [ ] Start the Docker environment and access the UI at `http://localhost:8080`
- [ ] Explore the UI: DAGs list, Grid view, Graph view, Calendar view, Logs
- [ ] Understand the three core processes: **webserver**, **scheduler**, **triggerer**
- [ ] Understand the **metadata database** (PostgreSQL) — what Airflow stores there
- [ ] Explore the Admin menu: Connections, Variables, Pools, Config

**Key concepts:** Airflow is a *scheduler*, not a data processor. It orchestrates work, it doesn't move data itself.

---

### 2. DAG Fundamentals

- [ ] Write your first DAG (`dags/01_hello_world.py`)
- [ ] Understand required DAG parameters:
  - `dag_id` — unique identifier
  - `start_date` — when the schedule begins (always use a static date, never `datetime.now()`)
  - `schedule` — cron string, timedelta, or `None` for manual-only
  - `catchup` — whether to backfill missed runs (set `False` when learning)
  - `tags` — for UI organization
- [ ] Task states: `queued`, `running`, `success`, `failed`, `skipped`, `up_for_retry`
- [ ] DAG run states and the difference between a **DAG run** and a **task instance**
- [ ] Task dependency syntax: `task_a >> task_b`, `task_b << task_a`, lists for fan-out/fan-in
- [ ] Understand `logical_date` (formerly `execution_date`) vs wall-clock time — this trips up everyone

**Practice:** Build a DAG with 5+ tasks in a diamond dependency pattern.

---

### 3. Core Operators

Operators are pre-built task templates. Each operator defines what a task *does*.

- [ ] **BashOperator** — run shell commands
- [ ] **PythonOperator** — call any Python function (`python_callable`)
- [ ] **BranchPythonOperator** — return a task_id (or list) to decide which branch runs
- [ ] **SimpleHttpOperator** — make HTTP requests
- [ ] **EmailOperator** — send emails (requires SMTP connection)
- [ ] **EmptyOperator** — a no-op, useful as a join/gate task
- [ ] Understand `op_args` and `op_kwargs` for passing arguments to operators
- [ ] Understand `retries`, `retry_delay`, and `execution_timeout` per task

**Practice:** Build a branching DAG that checks a condition and routes to different task chains.

---

### 4. TaskFlow API (Modern Approach)

Introduced in Airflow 2.0. Preferred way to write DAGs in 2026+.

- [ ] Use `@task` decorator instead of `PythonOperator`
- [ ] Use `@dag` decorator instead of the `DAG()` context manager
- [ ] Understand how TaskFlow automatically serializes return values as **XComs**
- [ ] Pass outputs directly between tasks: `result = task_a(); task_b(result)`
- [ ] Mix `@task` decorated tasks with classic operators in the same DAG
- [ ] Use `multiple_outputs=True` to return a dict and unpack its keys as separate XComs
- [ ] Type hints with TaskFlow — `@task` respects Python type hints

**Practice:** Rewrite the milestone 3 DAG entirely using TaskFlow API.

---

### 5. Sensors

Sensors are operators that **wait** for a condition to be true before proceeding.

- [ ] **FileSensor** — wait for a file to appear
- [ ] **HttpSensor** — wait for an HTTP endpoint to return a success response
- [ ] **ExternalTaskSensor** — wait for a task/DAG in another DAG to complete
- [ ] **DateTimeSensor** / **TimeDeltaSensor** — time-based waiting
- [ ] `mode='poke'` vs `mode='reschedule'` — poke holds a worker slot, reschedule frees it between checks
- [ ] `poke_interval` and `timeout` — always set both
- [ ] `soft_fail=True` — mark as skipped instead of failed on timeout

**Practice:** Build a DAG that waits for a file, then processes it, using reschedule mode.

---

### 6. XCom — Task Communication

XCom (cross-communication) lets tasks pass small values to downstream tasks.

- [ ] Manual push: `context['ti'].xcom_push(key='my_key', value='my_value')`
- [ ] Manual pull: `context['ti'].xcom_pull(task_ids='upstream_task', key='my_key')`
- [ ] Automatic XCom with TaskFlow (return value is automatically pushed)
- [ ] XCom size limits: metadata DB rows — keep XComs small (<48KB default), use S3/GCS for large data
- [ ] `do_xcom_push=False` on an operator to disable automatic return-value XCom
- [ ] View XComs in the UI: DAG run > task instance > XCom tab

**Practice:** Build an ETL DAG: extract task pushes a row count via XCom, transform task pulls it and conditionally branches.

---

### 7. Variables & Connections

- [ ] **Variables** — key-value config store in the metadata DB
  - Set via UI, CLI (`airflow variables set`), or env var (`AIRFLOW_VAR_<KEY>`)
  - `Variable.get('key', default_var='fallback')`
  - Mark as **Secret** in UI to mask in logs
- [ ] **Connections** — named credentials for external systems
  - Set via UI, CLI, or env var (`AIRFLOW_CONN_<CONN_ID>`)
  - Connection types: HTTP, Postgres, S3, GCP, Slack, etc.
  - Used by operators and hooks internally
- [ ] **Secrets Backend** — store sensitive values in Vault, AWS SSM, or GCP Secret Manager instead of the metadata DB
- [ ] **Pools** — limit concurrency for a group of tasks (e.g., max 5 DB connections)
- [ ] **Queues** — route tasks to specific workers (relevant with CeleryExecutor)

**Practice:** Store a base URL as a Variable and a credentials object as a Connection, then use both in a DAG.

---

### 8. Providers, Hooks & Custom Operators

Airflow's power comes from its **provider packages** — each is an installable PyPI package.

- [ ] Understand the provider model: `apache-airflow-providers-<name>`
- [ ] **Hooks** are the low-level connection abstractions; operators use hooks internally
  - `PostgresHook`, `S3Hook`, `HttpHook`, `SlackHook`
  - Use hooks directly in `@task` functions for clean code
- [ ] Write a **custom hook** (subclass `BaseHook`)
- [ ] Write a **custom operator** (subclass `BaseOperator`, implement `execute(self, context)`)
- [ ] Write a **custom sensor** (subclass `BaseSensorOperator`, implement `poke(self, context)`)
- [ ] Place custom operators/hooks/sensors in `plugins/` — auto-loaded by Airflow

**Key providers to know:** `postgres`, `http`, `amazon` (AWS), `google` (GCP), `dbt-cloud`, `spark`, `slack`, `email`

**Practice:** Write a custom operator that wraps an API call and a custom sensor that polls for job completion.

---

### 9. Dynamic Task Mapping

Introduced in Airflow 2.3. Run the same task over a dynamically-sized list without knowing the size at DAG-parse time.

- [ ] `task.expand(input=list_of_values)` — expand a task over a list
- [ ] `task.partial(fixed_arg="value").expand(dynamic_arg=list)` — fix some args, expand others
- [ ] Expand from an upstream task's XCom output
- [ ] **Mapped Task Groups** — expand a whole group of tasks together
- [ ] `map_index_template` — control how mapped instances are labeled in the UI
- [ ] Understand `max_map_length` config and performance considerations

**Practice:** Build a DAG that dynamically fetches from multiple API endpoints in parallel based on a config list.

---

### 10. Data-Aware Scheduling (Assets)

Introduced as "Datasets" in Airflow 2.4, renamed to "Assets" in Airflow 2.10. Event-driven scheduling based on data being produced.

- [ ] Define an **Asset**: `my_asset = Asset("s3://bucket/path/file.csv")`
- [ ] Mark a task as producing an asset: `outlets=[my_asset]`
- [ ] Schedule a DAG to run when an asset is updated: `schedule=[my_asset]`
- [ ] **Asset conditions** (AND/OR logic): `schedule=(asset_a & asset_b)`, `schedule=(asset_a | asset_b)`
- [ ] View asset lineage in the UI: Datasets/Assets menu
- [ ] Understand that assets decouple producer DAGs from consumer DAGs

**Practice:** Build two DAGs — a producer that marks an asset as updated, and a consumer that runs automatically when that asset is ready.

---

### 11. Scheduling Deep Dive

- [ ] **Cron expressions** — `"0 12 * * *"` (daily at noon), `"@daily"`, `"@hourly"`, presets
- [ ] `timedelta` schedules — `schedule=timedelta(hours=6)`
- [ ] **Data intervals**: each DAG run covers a window of time (`data_interval_start` → `data_interval_end`)
- [ ] `logical_date` is `data_interval_start` — the *start* of the period, not when the run triggers
- [ ] **Catchup**: when `catchup=True`, Airflow backfills all missed runs since `start_date`
- [ ] **Backfill via CLI**: `airflow dags backfill --start-date ... --end-date ...`
- [ ] **Custom Timetables** — define arbitrary schedules (e.g., business days only, irregular windows)
- [ ] `depends_on_past=True` — a task won't run unless the previous DAG run's same task succeeded

**Practice:** Create a DAG with `catchup=True` and a past `start_date`, trigger a backfill, observe multiple DAG runs created.

---

### 12. Deferrable Operators & Async Execution

Introduced in Airflow 2.2. Lets sensors/operators yield control while waiting, freeing worker slots.

- [ ] How deferral works: task suspends itself by raising `TaskDeferred`, the **triggerer** process watches for the condition, then resumes the task
- [ ] Deferrable versions of common sensors: `HttpSensorAsync`, `FileSensorAsync`, `TimeDeltaSensorAsync`
- [ ] Write a **custom Trigger** (subclass `BaseTrigger`)
- [ ] Why it matters: with classic sensors in `poke` mode, each waiting task occupies a worker slot — deferrable operators don't
- [ ] Configure the triggerer: `AIRFLOW__TRIGGERER__DEFAULT_CAPACITY`

**Practice:** Replace a classic sensor in one of your DAGs with its deferrable equivalent and observe the triggerer process handling it.

---

### 13. Executors

The executor decides *how* tasks are run.

- [ ] **SequentialExecutor** — one task at a time, SQLite DB, not for production
- [ ] **LocalExecutor** — parallel tasks via subprocess on the scheduler machine (this project uses this)
  - `AIRFLOW__CORE__PARALLELISM` — total concurrent tasks
  - `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG`
- [ ] **CeleryExecutor** — distribute tasks across multiple worker nodes via a message queue (Redis/RabbitMQ)
  - Add `redis` service + `airflow-worker` service to docker-compose to experiment
  - Flower for monitoring Celery workers
- [ ] **KubernetesExecutor** — each task runs in its own ephemeral Pod; scales to zero between runs
  - Each task can have its own Docker image, resources, and environment
- [ ] **LocalKubernetesExecutor** — hybrid: use Local for quick tasks, Kubernetes for heavy tasks

**Practice (Celery):** Extend the docker-compose with Redis + a Celery worker, switch the executor, and observe tasks distributed across workers.

---

### 14. Testing

- [ ] **DAG integrity tests** — validate every DAG loads without import errors and has no cycles
  ```python
  from airflow.models import DagBag
  def test_dags_load():
      dag_bag = DagBag(dag_folder="dags/", include_examples=False)
      assert not dag_bag.import_errors
  ```
- [ ] **Task unit tests** — test `execute()` directly with a mocked context
- [ ] `pytest-airflow` helpers for building task contexts in tests
- [ ] Test XCom push/pull by inspecting the `ti.xcom_pull()` result
- [ ] **DAG structure tests** — assert task counts, dependencies, and schedules are correct
- [ ] Test BranchPythonOperator routing by calling the python_callable directly

**Practice:** Write tests for all your milestone DAGs. Aim for a test that catches a broken dependency.

---

### 15. Monitoring & Alerting

- [ ] **SLA Misses** — `sla=timedelta(hours=2)` on a task; Airflow emails when the task exceeds it
  - `sla_miss_callback` on the DAG for custom handling
- [ ] **Callbacks**:
  - `on_failure_callback` — fires when a task fails
  - `on_success_callback` — fires when a task succeeds
  - `on_retry_callback` — fires on each retry
  - `on_skipped_callback` — fires when a task is skipped
  - Set on individual tasks or on the DAG (applies to all tasks)
- [ ] **Email on failure** — `email_on_failure=True`, `email=['ops@example.com']`
- [ ] **Metrics with StatsD** — Airflow emits metrics for task durations, success rates, queue depth
  - Wire up Prometheus + Grafana for dashboards
- [ ] **Health endpoints** — `GET /health` on webserver and scheduler for uptime monitoring

**Practice:** Add `on_failure_callback` to a DAG that logs a Slack message (use `SlackHook`) when any task fails.

---

### 16. Best Practices & Production Patterns

- [ ] **Idempotency** — tasks should produce the same result if run multiple times; use `INSERT OR REPLACE`, `TRUNCATE + INSERT`, or check-before-write patterns
- [ ] **Atomicity** — each task should do one logical unit of work; avoid tasks that do too much
- [ ] **No dynamic logic at parse time** — DAG files are parsed constantly; expensive calls (DB queries, API calls) at module level will slow the scheduler
- [ ] **Use `start_date` in the past, never `datetime.now()`** — leads to unpredictable behavior
- [ ] **Always set `catchup=False`** unless backfilling is explicitly desired
- [ ] **`max_active_runs`** — limit how many DAG runs can execute concurrently
- [ ] **CI/CD for DAGs** — lint with Ruff, run DAG integrity tests in CI before deploying
- [ ] **Secrets management** — never hardcode credentials; use Connections, Variables, or a Secrets Backend
- [ ] **DAG folder organization** — group related DAGs in subdirectories (Airflow 2.x+ supports this)
- [ ] **`.airflowignore`** — exclude files/folders from DAG parsing (like `tests/`, `__init__.py`)
- [ ] **Template fields** — use Jinja templates (`{{ ds }}`, `{{ data_interval_start }}`) for dynamic values instead of Python closures

**Practice:** Audit your existing DAGs against this checklist and refactor any violations.

---

### 17. Airflow 3.x Features

This project runs Airflow 3.2.2. These are the major changes from the 2.x era that you'll encounter while working through the earlier milestones:

- [ ] **New React UI** — completely rebuilt frontend; DAG/task views, Grid, Graph, and Calendar all reworked
- [ ] **Asset-centric UI** — lineage and asset status are first-class concepts in the nav (replaces the old Datasets menu)
- [ ] **Edge Executor** — lightweight executor for edge/embedded deployments alongside Local/Celery/Kubernetes
- [ ] **Backfill API** — programmatic backfills via the REST API, no longer CLI-only
- [ ] **Task execution API** — decoupled task execution; enables better remote execution and custom worker environments
- [ ] **Auth manager interface** — FAB replaced; the default ships with a new auth manager. `AIRFLOW__API__AUTH_BACKENDS` is gone; auth is configured via `AIRFLOW__CORE__AUTH_MANAGER`
- [ ] **Breaking changes from 2.x** (relevant if migrating existing DAGs):
  - `execution_date` fully removed — use `logical_date` everywhere
  - `SubDagOperator` removed — use `TaskGroup` instead
  - Several long-deprecated operators and hooks removed (check release notes before migrating)
  - Python 3.8 and 3.9 dropped; Python 3.10+ required

**Reference:** [Airflow 3.x release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html) — check this when a 2.x example from the internet doesn't work as expected.

---

## Useful Commands

```bash
# List all DAGs
docker compose exec airflow-webserver airflow dags list

# Trigger a DAG run manually
docker compose exec airflow-webserver airflow dags trigger <dag_id>

# Test a single task (dry-run, no DB state)
docker compose exec airflow-webserver airflow tasks test <dag_id> <task_id> 2024-01-01

# Backfill a date range
docker compose exec airflow-webserver airflow dags backfill -s 2024-01-01 -e 2024-01-07 <dag_id>

# Pause/unpause a DAG
docker compose exec airflow-webserver airflow dags pause <dag_id>
docker compose exec airflow-webserver airflow dags unpause <dag_id>

# View task logs
docker compose exec airflow-webserver airflow tasks logs <dag_id> <task_id> <logical_date>

# List connections
docker compose exec airflow-webserver airflow connections list

# Add a connection
docker compose exec airflow-webserver airflow connections add my_conn \
  --conn-type http --conn-host https://api.example.com

# Set a variable
docker compose exec airflow-webserver airflow variables set my_key my_value

# Check scheduler health
docker compose exec airflow-scheduler airflow jobs check
```

---

## Useful Jinja Template Variables

Inside operator template fields (`bash_command`, SQL strings, etc.):

| Variable | Value |
|----------|-------|
| `{{ ds }}` | Logical date as `YYYY-MM-DD` |
| `{{ ds_nodash }}` | Logical date as `YYYYMMDD` |
| `{{ logical_date }}` | Full logical datetime (pendulum) |
| `{{ data_interval_start }}` | Start of the data interval |
| `{{ data_interval_end }}` | End of the data interval |
| `{{ dag.dag_id }}` | The DAG's ID |
| `{{ task.task_id }}` | The task's ID |
| `{{ run_id }}` | The DAG run ID |
| `{{ var.value.my_key }}` | Airflow Variable value |
| `{{ conn.my_conn.host }}` | Connection field |

---

## Resources

- [Official Docs](https://airflow.apache.org/docs/)
- [Airflow GitHub](https://github.com/apache/airflow)
- [Provider Packages Index](https://airflow.apache.org/docs/#providers-packages-docs-apache-airflow-providers)
- [Astronomer Registry](https://registry.astronomer.io/) — community DAGs and operator examples
- [Astronomer Blog](https://www.astronomer.io/blog/) — deep dives on Airflow internals
