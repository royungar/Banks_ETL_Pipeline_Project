# ETL Banks Data Pipeline Project – IBM Data Engineering Professional Certificate

## Overview

This project demonstrates the development of an ETL (Extract, Transform, Load) pipeline using **Python** to process data on the world’s largest banks.
It was completed as the final project for **Course 3 – Python Project for Data Engineering**
in the [IBM Data Engineering Professional Certificate](https://www.coursera.org/professional-certificates/ibm-data-engineer).

---

## Objectives

- Extract tabular data on the largest banks by market capitalization from a snapshot of a Wikipedia page.
- Transform the market cap data from USD to GBP, EUR, and INR using exchange rates from a CSV file.
- Load the final transformed data into:
  - A structured CSV file
  - A local SQLite database table
- Run SQL queries on the database for downstream analysis.
- Log each ETL step in a timestamped log file for traceability and monitoring.

---

## Tools & Technologies

| Category              | Tools / Libraries                                           |
|-----------------------|-------------------------------------------------------------|
| **Programming**       | Python 3.11                                                 |
| **Libraries**         | `pandas`, `numpy`, `sqlite3`, `requests`, `bs4`, `datetime` |
| **Database**          | SQLite                                                      |
| **Data Sources**      | Wikipedia snapshot, CSV (exchange rates)                    |

---

## ETL Pipeline Components

### 1. Extract

- Uses the `requests` library to download HTML from the [archived Wikipedia page](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks).
- `BeautifulSoup` is used to locate and extract the correct table (`class="wikitable sortable"`).
- The table is parsed into a `pandas` DataFrame.

### 2. Transform

- Exchange rate data is loaded from a CSV (`exchange_rate.csv`).
- The market capitalization in USD is converted into:
  - GBP (`MC_GBP_Billion`)
  - EUR (`MC_EUR_Billion`)
  - INR (`MC_INR_Billion`)
- The result is a clean DataFrame with added columns for each currency.

### 3. Load

- The transformed data is saved to:
  - A CSV file: `Largest_banks_data.csv`
  - A SQLite database: `Banks.db`, with table name `Largest_banks`

### 4. Query

Three SQL queries are executed using `pandas.read_sql()`:
- `SELECT * FROM Largest_banks`
- `SELECT AVG(MC_GBP_Billion) FROM Largest_banks`
- `SELECT "Bank name" FROM Largest_banks LIMIT 5`

### 5. Logging

- Each step of the pipeline appends a timestamped message to `code_log.txt` using a custom logging function.

---

## Repository Structure

```plaintext
ETL_Banks_Data_Pipeline_Project/
├── README.md                     # Project documentation (this file)
├── data/
│   ├── exchange_rate.csv         # Input exchange rates
│   ├── Largest_banks_data.csv    # Transformed output CSV
│   └── Banks.db                  # SQLite DB with final data
├── logs/
│   └── code_log.txt              # Timestamped log of ETL execution
├── python/
│   └── banks_project.py          # Main ETL pipeline script
```

---

## How to Run

1. Ensure you have Python 3.11 installed with the required libraries:

   ```bash
   pip install pandas numpy requests beautifulsoup4
   ```

2. Place the following input file in the `data/` folder:

   - `exchange_rate.csv` (provided)

3. Run the ETL script:

   ```bash
   python3 python/banks_project.py
   ```

4. Outputs will be saved to:

   - `data/Largest_banks_data.csv`
   - `data/Banks.db`
   - `logs/code_log.txt`

---

## Output Preview

**Sample from `Largest_banks_data.csv`:**

| Rank | Bank name                | MC_USD_Billion | MC_GBP_Billion | MC_EUR_Billion | MC_INR_Billion |
|------|--------------------------|----------------|----------------|----------------|----------------|
| 1    | JPMorgan Chase           | 400.0          | 320.0          | 368.0          | 33280.0        |
| 2    | Bank of America          | 300.0          | 240.0          | 276.0          | 24960.0        |
| ...  | ...                      | ...            | ...            | ...            | ...            |

---

## License

This project was completed as part of the IBM Data Engineering Professional Certificate and is intended for educational use.

## Links

- Course Page - [Python Project for Data Engineering](https://www.coursera.org/learn/python-project-for-data-engineering)
- [GitHub Profile](https://github.com/royungar)
- [GitHub Repository](https://github.com/royungar/ETL_Banks_Data_Pipeline_Project)