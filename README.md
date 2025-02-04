# Personal Finance Organizer

## Overview

**Personal Finance Organizer** is a comprehensive tool designed to help individuals manage their finances by aggregating data from various financial accounts, storing it securely, and visualizing spending patterns through interactive dashboards. The project integrates multiple technologies to streamline financial data processing, storage, and visualization.

By leveraging the **Plaid API**, this tool securely pulls transaction and investment data from connected financial institutions. The data is stored in **Google BigQuery** for querying and analysis. A **Tableau** dashboard is used to present insights, giving a clear overview of spending habits and financial health.

## Features

- **Secure Financial Data Aggregation**: Connect multiple financial accounts via the Plaid API.
- **Cloud-Based Data Storage**: Store and query financial data using Google BigQuery.
- **Interactive Dashboards**: Visualize spending, income, and investment data with Tableau.
- **Automated Budget Tracking**: Monitor budget performance against set thresholds.
- **Data Quality Alerts**: Receive notifications for anomalies in data using SendGrid.
- **Modular Design**: Modular code structure for easy customization and scaling.

## Technologies Used

- **Plaid API**: For securely fetching financial data from banks and other institutions.
- **Google BigQuery**: For storing and analyzing large datasets efficiently.
- **Tableau**: For creating interactive dashboards to visualize financial data.
- **SendGrid API**: For sending automated alerts regarding data quality and budget performance.
- **Python**: Main programming language used for data ingestion, processing, and automation.
- **Google Secrets Manager**: Used for securely managing secrets and configuration settings.
- **Google Cloud PubSub**: To generate a data pipeline that runs after each successive job.
- **Google Cloud Scheduler**: For scheduling jobs to run on a daily cadence.
- **Google Cloud Functions**: To run the python scripts for data processing.
- **Jupyter Notebooks**: For exploratory data analysis and testing.

## How It Works

- **Data Ingestion**: The project connects to your financial institutions using **Plaid API**, fetching transaction, account, and investment data. This is handled by the `PlaidUtils` module in the `utils/` directory.
- **Data Storage**: Fetched data is structured and loaded into Google BigQuery using the `BqUtils` module. Custom schemas are defined in the `schemas/` directory to ensure proper formatting and organization of data.
- **Data Processing & Alerts**: Automated jobs (`jobs/`) process the data for budgeting, account summaries, and anomaly detection. If irregularities are detected, alerts are sent via SendGrid.
- **Visualization**: The processed data is visualized through a **Tableau** dashboard, providing insights into your spending patterns, income distribution, and overall financial health.

## Setup Instructions

### Prerequisites

- **Python 3.x**
- **Google Cloud SDK** (for BigQuery access)
- **Tableau Desktop** (for dashboard visualization)
- **Plaid API credentials** (sign up at [Plaid](https://plaid.com/docs/auth/))

### Installation

1. Clone the Repository

```bash
git clone https://github.com/zcox10/personal_finance.git
cd personal_finance
```

2. Set Up Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

3. Install Dependencies

```bash
pip install -r requirements.txt
```

4. **Run the Project**: `python main.py`
