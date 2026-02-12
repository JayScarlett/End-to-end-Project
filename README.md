# Data Analytics Warehouse

**PostgreSQL · SQL · Python · Power BI · ETL · Dimensional Modeling**

---

## Overview

This project demonstrates a complete analytics engineering workflow from transactional data ingestion to dimensional warehouse modeling and executive reporting.

It emphasizes practical data warehouse design principles including fact table grain enforcement, continuous calendar implementation, SQL-based transformations, ETL automation with Python, and BI-ready semantic modeling.

The final deliverable is a star schema data warehouse optimized for accurate aggregation, time intelligence, and KPI reporting in Power BI.

This version is a redesign of an earlier iteration, introducing a cleaner dimensional architecture and a formal data validation layer.

---

## Architecture

The project follows a layered warehouse design aligned with modern analytics practices.

### Raw Layer  
Source-aligned tables ingested into PostgreSQL.

### Core Relational Layer  
A normalized schema designed to:

- Preserve transactional grain  
- Enforce referential integrity  
- Maintain source-of-truth fidelity  

This layer prioritizes correctness and structural integrity.

### Analytics Layer (Star Schema)

Dimensional model optimized for reporting and BI tools:

**Fact**
- `order_fact`  
  - Grain: one row per `order_id` + `order_item_id`

**Dimensions**
- `date_dim` (continuous calendar table, no gaps)
- `customer_dim`
- `seller_dim`
- `product_dim`

**Design Decisions**
- Explicit fact grain enforcement to prevent duplication  
- Continuous date dimension to support time intelligence  
- Derived operational metrics stored in the fact table  
- Clean one-to-many dimensional relationships  

This structure enables efficient filtering, aggregation, and KPI calculation in Power BI.

---

## ETL & Data Engineering

The warehouse can be rebuilt end-to-end using:

- SQL DDL scripts for schema creation  
- SQL-based dimensional transformations  
- Python-based ETL orchestration (SQLAlchemy, dotenv)  
- Environment-based configuration for secure credential management  

This demonstrates:

- Data pipeline structuring  
- SQL transformation logic  
- Reproducibility and automation  
- Practical analytics engineering fundamentals  

---

## Data Validation

Before building dashboards, the warehouse is validated using SQL-based checks:

- Row count reconciliation (raw vs fact tables)  
- Revenue reconciliation  
- Duplicate grain detection  
- Referential integrity validation  
- Continuous calendar enforcement  
- Derived metric sanity checks  

These validations ensure downstream analytics are accurate and reliable.

---

## Business Intelligence (Power BI)

The dimensional warehouse feeds an executive-level dashboard.

### Executive Overview

**KPIs**
- Total Revenue  
- Revenue per Order  
- Average Delivery Days  
- Late Delivery Rate  

**Visuals**
- Revenue trend over time  
- Top revenue-driving product categories  

The BI model includes DAX measures, star-schema relationships, and interactive filtering to support business decision-making.

---

## Repository Structure
ETL/
├── sql/ # DDL and dimensional load scripts
├── olist_etl.py # Python ETL orchestration
└── DDL Images/ # Schema diagrams

EDA/
└── Post ETL Data Validation.py # Validation checks

PowerBI/
└── Executive Dashboard.pbix # BI dashboard

---

## Technologies

- PostgreSQL  
- SQL  
- Python  
- Power BI  
- Git / GitHub  

---

## Key Concepts Demonstrated

- Dimensional modeling (star schema design)  
- Fact table grain enforcement  
- Continuous calendar implementation  
- SQL transformation pipelines  
- ETL automation  
- Data validation prior to reporting  
- KPI definition and business metric modeling  

---