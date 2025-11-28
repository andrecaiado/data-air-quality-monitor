# Data Air Quality Monitor - Databricks Project
This project is a data air quality monitoring solution running in Databricks. It includes the necessary configurations to deploy resources such as SQL warehouses, datasets, and dashboards in a Databricks workspace.

This project ingests data from OPENAQ Platform (https://openaq.org/) to monitor air quality metrics.

> [!NOTE]
> This project is designed to be deployed in Databricks Free Edition, which has certain limitations. As such, some configurations and paths have been adjusted accordingly.

## Features
- Ingestion of air quality data from OPENAQ Platform.
- Configuration of Databricks SQL warehouse for data processing.
- Creation of datasets and dashboards for visualizing air quality metrics.
- Alerting setup for monitoring air quality thresholds.

## Requirements
- Databricks account with necessary permissions to create resources in the workspace.
- Python 3.7 or higher.
- An OPENAQ API key.
- Some Databricks extension for your IDE (optional, for easier management).
  - For VSCode, you can use the "Databricks" extension by Databricks.

## Project Structure
- `databricks.yml`: Configuration file for the Databricks Bundle.
- `air-quality-dashboard.dev.lvdash.json`: Dashboard configuration file for the development environment.
- `measurements-above-limit.dev.dbalert.json`: Alert configuration file for the development environment. This file is not being deployed by the Databricks Bundle and so, it must be managed separately.
- `.env.template`: Template for environment variables, to use for local development.
- Other necessary scripts and configurations for data ingestion and processing.
  - `ingest_bronze_locations.py`: Script to ingest locations data (from OPENAQ Platform).
  - `build_silver_dimensions.py`: Script to extract locations and sensors data from bronze table and build silver dimension tables.
  - `ingest_bronze_measurements.py`: Script to ingest measurement data (from OPENAQ Platform) into the bronze table.
  - `transform_silver_measurements.py`: Script to transform measurement bronze data into silver tables.
  - `build_gold_measurements.py`: Script to build the gold table with aggregated data for analysis and reporting.
- `requirements.txt`: Project python dependencies. This is necessary so the `data_air_quality_monitor` job can install the required dependencies to execute the scripts in the Databricks environment.
- `config/`: Directory containing files for configuration management.
  - `settings.py`: Python module to load and manage configuration settings.

## Setup Instructions for local development

To set up the project for local development, follow these steps:

1. Clone the repository to your local machine:
   ```bash
   git clone https://github.com/andrecaiado/data-air-quality-monitor.git
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

## Deploy the project to a Databricks workspace

To deploy the project to a Databricks workspace, use the Databricks Bundle Configuration. The [databricks.yml](./databricks.yml) file contains the configuration for the Databricks Bundle, defining resources such as SQL warehouses, datasets, and dashboards. As the file only contains one target environment defined (`dev`), this deployment example will be targeting that environment.

The steps to deploy the project are as follows:

1. Configure Databricks CLI with your workspace URL and token:
   ```bash
   databricks configure --host https://<your-databricks-workspace-url> --token
   ```

2. Create the scoped secret for the OPENAQ API key in your Databricks workspace:
   ```bash
   databricks secrets create-scope --scope data-air-quality-monitor
   databricks secrets put --scope data-air-quality-monitor --key OPENAQ_API_KEY
   ```

3. Create the configuration file in your Workspace:

> [!NOTE] 
> Pure JSON upload (raw) is not supported via workspace import on Free Edition. Converting to a Python source file avoids the format mismatch.

> [!IMPORTANT] 
> Don't add the `.py` extension to the file name when imported to the workspace. Setting the format to `SOURCE` and language to `PYTHON` ensures that the file is treated as a Python source file. > Adding the `.py` extension to the file name in the workspace would result in a duplicate extension (e.g., `config_dev.py.py`), which can lead to import errors.

   ```bash
   echo 'CONFIG = {
      "DATABASE": "dev_airq",
      "OPENAQ_API_V3_BASE_URL": "https://api.openaq.org/v3"
   }' > config_dev.py
   databricks workspace mkdirs /Workspace/Users/<your-email>/data-air-quality-monitor-config
   databricks workspace import /Workspace/Users/<your-email>/data-air-quality-monitor-config/config_dev \
      --file config_dev.py \
      --format SOURCE \
      --language PYTHON \
      --overwrite
   ```

4. Create the Databricks SQL warehouse (if not already created):
   ```bash
   databricks sql warehouses create --name airq-dev --cluster-size 2X-Small --auto-stop-minutes 10
   ```

5. Update the `databricks.yml` file with your Databricks workspace URL, SQL warehouse ID, and your email in the script parameters.

6. Install the Databricks Bundle CLI if you haven't already:
   ```bash
   pip install -U databricks-bundle
   ```

7. Validate the bundle configuration:

   ```bash
   databricks bundle validate --target dev
   ```

8. Deploy the bundle:
   ```bash
   databricks bundle deploy --target dev
   ```

9. (Optional) To destroy the deployed resources:
   ```bash
   databricks bundle destroy --target dev
   ```