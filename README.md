# Eligibility Data Pipeline

This project implements a scalable, configuration-driven data pipeline that ingests partner eligibility files, standardizes them into a unified schema, and outputs a consolidated dataset.

The pipeline is built using PySpark and follows a Medallion-style architecture:

raw → bronze → silver → gold

---

## Architecture

### Raw
Partner source files in their original format.

### Bronze
- Reads partner files using config-defined delimiters
- Adds `partner_code` and ingestion timestamp
- Stores data in Parquet

### Silver
- Maps partner columns to standard schema
- Applies transformations:
  - Names → Title Case
  - Emails → lowercase
  - DOB → ISO format
  - Phone → `XXX-XXX-XXXX`
- Drops rows missing required identifiers

### Gold
- Unifies all Silver datasets into one table

---

## Standard Output Schema

| Field | Description |
|------|-------------|
external_id | Partner member identifier |
first_name | First name |
last_name | Last name |
dob | Date of birth (YYYY-MM-DD) |
email | Email (lowercase) |
phone | Phone (XXX-XXX-XXXX) |
partner_code | Hardcoded partner identifier |

---

## How to Run

### Requirements
- Python 3.11+
- Java 17
- PySpark
- PyYAML

---

### Setup


Clone the repository and move into the project directory:

```bash
git clone https://github.com/harshithadsuno/eligibility-pipeline.git
cd eligibility-pipeline
