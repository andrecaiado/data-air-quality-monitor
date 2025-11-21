# Data Air Quality Monitor - Databricks Project
This project is a data air quality monitoring solution running in Databricks. It includes the necessary configurations to deploy resources such as SQL warehouses, datasets, and dashboards in a Databricks workspace.

This project ingests data from OPENAQ Platform (https://openaq.org/) to monitor air quality metrics.

## Features
- Ingestion of air quality data from OPENAQ Platform.
- Configuration of Databricks SQL warehouse for data processing.
- Creation of datasets and dashboards for visualizing air quality metrics.
- Alerting setup for monitoring air quality thresholds.

## Requirements
- Databricks CLI installed and configured.
- Access to a Databricks workspace.
- Python 3.7 or higher.
- Necessary permissions to create resources in the Databricks workspace.
- An OPENAQ API key (if required for extended access).
- Some Databricks extension for your IDE (optional, for easier management).
  - For VSCode, you can use the "Databricks" extension by Databricks.

## Project Structure
- `databricks.yml`: Configuration file for the Databricks Bundle.
- `Air quality dashboard.lvdash.json`: Dashboard configuration file.
- .env.template: Template for environment variables.
- Other necessary scripts and configurations for data ingestion and processing.

## Setup Instructions for local development

To set up the project for local development, follow these steps:

1. Clone the repository to your local machine:
   ```bash
   git clone https://github.com/your-username/data-air-quality-monitor.git
   cd data-air-quality-monitor
   ```
2. Install the Databricks CLI if you haven't already:
   ```bash
   pip install databricks-cli
   ```
3. Configure the Databricks CLI with your workspace URL and token:
   ```bash
   databricks configure --host https://<your-databricks-workspace-url> --token
   ```
4. Create a `.env` file based on the `.env.template` and set the required environment variables.

5. Install Databricks Connect if you plan to run code locally that interacts with your Databricks cluster:
   ```bash
   pip install -U databricks-connect
   ```
6. Configure Databricks Connect to point to your cluster:
   ```bash
   databricks-connect configure
   ```

## Deploy the project 

To deploy the project to a Databricks workspace, use the Databricks Bundle Configuration. The [databricks.yml](./databricks.yml) file contains the configuration for the Databricks Bundle, defining resources such as SQL warehouses, datasets, and dashboards.

The steps to deploy the project are as follows:

1. Configure Databricks CLI with your workspace URL and token:
   ```bash
   databricks configure --host https://<your-databricks-workspace-url> --token
   ```

2. Validate the bundle for the desired target environment:

   ```bash
   databricks bundle validate --target dev
   ```

3. Deploy the bundle to the target environment:
   ```bash
   databricks bundle deploy --target dev
   ```

4. To destroy the deployed resources in the target environment:
   ```bash
   databricks bundle destroy --target dev
   ```