# End-to-End E-Commerce Analytics Project

## Project Overview

This project demonstrates an end-to-end analytics workflow using a real-world e-commerce dataset. It follows a production-style approach, beginning with database design and ETL, progressing through exploratory data analysis, and culminating in business-focused insights delivered through interactive dashboards.

“The objective is to demonstrate ownership of the full analytics lifecycle: building reliable data foundations, validating data behavior, and translating findings into decision-support artifacts.”
---

## Data Engineering and ETL

This phase focuses on building a reliable and scalable analytics foundation.

Key components include:
- Relational database design and schema normalization
- Construction of an analytics star schema
- SQL-based data loading and transformation
- Python ETL pipelines for reproducible ingestion
- Data quality checks and anomaly handling

This layer serves as the single source of truth for downstream analysis and reporting.

---

## Exploratory Data Analysis (EDA)

This phase validates the analytics layer and explores key patterns in the data using Python.

Key activities include:
- Time-series analysis of orders and revenue at daily and monthly levels
- Distribution analysis of order and payment values
- Identification of anomalies, outliers, and dataset boundaries
- Validation of aggregation logic used in analytics tables

The EDA phase informs which metrics and dimensions are meaningful for business analysis.

---

## Business Analysis and Decision Support (In Progress)

This phase builds on the validated analytics layer and EDA findings to answer concrete business questions using stakeholder-oriented visuals and metrics.

### What is complete
- Analytics star schema finalized and validated
- Fact and dimension tables loaded into the analytics layer
- Time-series EDA completed in Python
- Key anomalies and dataset boundaries identified
- Power BI semantic model established

### What this phase will deliver
- Executive-level KPIs and growth trends
- Revenue and order performance by time, customer, and category
- Operational insights related to delivery performance and customer experience
- Interactive Power BI dashboards designed for business decision-making

### Planned business questions
- How has revenue scaled relative to order volume over time?
- Which segments contribute disproportionately to total revenue?
- What operational factors correlate with customer satisfaction?
- Are observed growth patterns seasonal or structural?

This phase emphasizes insight generation and business interpretation rather than additional data modeling.
