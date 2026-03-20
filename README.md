# symmy-task

---

* The `sync_products` task reads ERP JSON files and synchronizes products with the e-shop API.
* Different dataset names (e.g., `erp_data`, `erp_data_2`, `erp_data_3`, `erp_data_4`) allow testing multiple import scenarios.

---

# Introduction:
* Extract: get_erp_data() reads ERP JSON exports and loads them into Python structures.
* Validation: erp_data_quality.py consist of two functions validate_items, consistent_items to separate valid and invalid records.
* Transformation: transform_erp_data() converts valid data from ERP into format of the e-shop API, calculates VAT price, aggregates stock and normalizes attributes.
* Change Detection: get_hash() generates SHA256 hashes of product data and compares them with stored hashes to avoid unnecessary updates.
* Data Quality Logging: invalid items are stored in the DataQualityLog model with error messages.
* Synchronization: Products are sent to the external API using POST (create) or PATCH (update).
* Retry Logic: send_request() retries API calls when HTTP 429 rate limits occur (set to 10).
* Persistence: ProductSync stores the last synchronized product data and hashes.

---

<details>
<summary>symmy-task/</summary>

---

# integrator.task.py description:
1. Load data function: Open data from local disc
2. Transformation function: 
    1) adds 21% VAT to variable price_vat_excl, 
    2) aggregate (sums) stocks if, there are more dicts (in list of dicts) with same ID
    3) sets color attributes to {'color': "N/A"} if missing
3. Hash function: Create hashes for each dict in list if dicts
4. Preprocessing data function: validation, splits dataset into invalid and valid items. Ivalid items are send to DB (SELECT * FROM integrator_dataqualitylog LIMIT 10;). Valid items are transformed and hashed. The RETURN of this function are validated, transformed, hashed data.
5. Sync function: check DB, based on results from this check POST/PATCH and update DB.
    SELECT * FROM integrator_productsync LIMIT 10;
6. Main function: loads ERP data, transforms it, and dispatches per-SKU tasks.

---

# test_dqf_tranform.py description:
1. Validation function - splits erp_data into two sets:
    1) valid_data - this dataset is send to eshop API
    2) invalid_data - this dataset is not send to eshop API, quality errors are uploaded to db
    List of checks:
    1) item is not dict
    2) id or title is not in item
    3) price_vat_excl is None or price_vat_excl <= 0
    4) stocks in item is not dict
    5) stocks counts are invalid values
2. Consistency function - returns sku_id with inconsistent values of:
    1) title, 
    2) price_vat_excl,
    3) attributes

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

# sync_products(data_set_name type str, mock type bool) 
* to test sync DB use mock = True
* to test retry calls if response status is 429 use mock = False

Run sync with specific ERP file:

```python: call main function
sync_products('erp_data', True) 
```
```
sync_products('erp_data', False) 
```

Run sync with second ERP dataset:

```python: call main function
sync_products('erp_data_2', True) 
```
```
sync_products('erp_data_2', False) 
```

Run sync with third ERP dataset:

```python: call main function
sync_products('erp_data_3', True) 
```
```
sync_products('erp_data_3', False) 
```

Run sync with fourth ERP dataset:
```python: call main function
sync_products('erp_data_4', True) 
```
```
sync_products('erp_data_4', False) 
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


