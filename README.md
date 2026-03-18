# symmy-task

---

* The `sync_products` task reads ERP JSON files and synchronizes products with the e-shop API.
* Different dataset names (e.g., `erp_data`, `erp_data_2`, `erp_data_3`) allow testing multiple import scenarios.

---

# task.py description:
1. Extract: get_erp_data() reads ERP JSON exports and loads them into Python structures.
2. Validation: erp_data_quality.py consist of two functions validate_items, consistent_items to separate valid and invalid records
3. Transformation: transform_erp_data() converts valid data from ERP into format of the e-shop API, calculates VAT price, aggregates stock and normalizes attributes.
4. Change Detection: get_hash() generates SHA256 hashes of product data and compares them with stored hashes to avoid unnecessary updates.
5. Data Quality Logging: invalid items are stored in the DataQualityLog model with error messages.
6. Synchronization: Products are sent to the external API using POST (create) or PATCH (update).
7. Retry Logic: send_request() retries API calls when HTTP 429 rate limits occur.
8. Persistence: ProductSync stores the last synchronized product data and hashes.

---

# Product Integrator – Setup & Run Guide

This project uses **Docker** and **Docker Compose** to run the application and its dependencies.

---

# 1. Check Prerequisites

Make sure Docker and Docker Compose are installed.

```bash
docker --version
docker compose version
```

---

# 2. Build Containers

Build the project images:

```bash
docker compose build
```

---

# 3. Start the Application

Run the containers.

Detached mode (recommended):

```bash
docker compose up -d 
```

Or:

```bash
docker compose up --build -d
```

Or run with logs visible:

```bash
docker compose up
```

---

# 4. Run Database Migrations

Create and apply migrations for the `integrator` app.

```bash
docker compose exec web python manage.py makemigrations integrator
docker compose exec web python manage.py migrate
```

---

# 5. Run the Product Sync Task

Open Django shell:

```bash
docker compose exec web python manage.py shell
```

Inside the shell:

```python
from integrator.tasks import sync_products
```

Run sync with specific ERP file:

```python
sync_products('erp_data')
```

Run sync with another ERP dataset:

```python
sync_products('erp_data_2')
```

Exit the shell:

```python
exit()
```

---

# 6. Stop the Application

Stop and remove the containers:

```bash
docker compose down
```

---


