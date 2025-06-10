# ActiveFence Data Pipeline

This project implements a data pipeline for collecting, ingesting, and enriching textual data from various online sources, storing the processed information in a Delta Lake. The pipeline is containerized using Docker and utilizes Apache Spark for scalable data processing.

## Table of Contents

  - [ActiveFence Data Pipeline](https://www.google.com/search?q=%23activefence-data-pipeline)
      - [Table of Contents](https://www.google.com/search?q=%23table-of-contents)
      - [1. Project Overview](https://www.google.com/search?q=%231-project-overview)
      - [2. Architecture Diagram](https://www.google.com/search?q=%232-architecture-diagram)
      - [3. Data Flow Description](https://www.google.com/search?q=%233-data-flow-description)
      - [4. Schema Designs](https://www.google.com/search?q=%234-schema-designs)
          - [4.1. `text_table` (Delta Lake)](https://www.google.com/search?q=%2341-text_table-delta-lake)
          - [4.2. `threat_mapping` (Delta Lake)](https://www.google.com/search?q=%2342-threat-mapping-delta-lake)
          - [4.3. `enriched_table` (Delta Lake)](https://www.google.com/search?q=%2343-enriched-table-delta-lake)
      - [5. Setup and Execution](https://www.google.com/search?q=%235-setup-and-execution)
          - [5.1. Prerequisites](https://www.google.com/search?q=%2351-prerequisites)
          - [5.2. Project Structure](https://www.google.com/search?q=%2352-project-structure)
          - [5.3. Configuration](https://www.google.com/search?q=%2353-configuration)
          - [5.4. Building the Docker Images](https://www.google.com/search?q=%2354-building-the-docker-images)
          - [5.5. Running the Pipeline](https://www.google.com/search?q=%2355-running-the-pipeline)
          - [5.6. Cleaning Up Data](https://www.google.com/search?q=%2356-cleaning-up-data)
      - [6. Assumptions and Design Decisions](https://www.google.com/search?q=%236-assumptions-and-design-decisions)
          - [6.1. Core Assumptions](https://www.google.com/search?q=%2361-core-assumptions)
          - [6.2. Design Decisions](https://www.google.com/search?q=%2362-design-decisions)
          - [6.3. Scope Limitations and Acknowledged Omissions](https://www.google.com/search?q=%2363-scope-limitations-and-acknowledged-omissions)
      - [7. Future Improvements](https://www.google.com/search?q=%237-future-improvements)

## 1\. Project Overview

The ActiveFence project establishes a robust data pipeline designed to:

  * **Collect** raw textual data from various online sources (e.g., blogs, forums).
  * **Ingest** this data into a structured format within a Delta Lake.
  * **Enrich** the ingested data by applying transformation, threat mapping, and potentially other NLP techniques.
  * **Store** the enriched data in a separate Delta Lake table for subsequent analysis and reporting.

The pipeline is built with scalability in mind, leveraging Apache Spark for data processing and Delta Lake for reliable, versioned data storage.

## 2\. Architecture Diagram

```mermaid
graph TD
    A[Collectors] --> B{Raw Data (data/raw)};
    B --> C[Spark Ingestion];
    C --> D{Delta Lake: text_table};
    D --> E[Spark Enrichment];
    E --> F{Delta Lake: enriched_table};
    subgraph Data Sources
        A -- dev.to --> A;
        A -- nakedcapitalism.com --> A;
        A -- reddit.com --> A;
    end
    subgraph Spark Services
        C -- text_ingest.py --> C;
        C -- threat_mapping_ingest.py --> C;
        E -- stream_text_enrichment.py --> E;
    end
    subgraph Logs
        B -- collectors.log --> L;
        C -- ingestion.log --> L;
        E -- stream_enrichment.log --> L;
    end
    L[Logs Folder]
```

## 3\. Data Flow Description

1.  **Collection:**

      * The `collect_n_ingest` Docker service initiates the data collection.
      * Python scripts in the `collectors/` directory (`dev_to.py`, `nakedcapitalism_com.py`, `reddit_com.py`) are executed to scrape data from specified online sources.
      * Collected raw data is temporarily stored in the `data/raw` directory.
      * Collection logs are written to `logs/collectors.log`.

2.  **Ingestion:**

      * Immediately following collection, the `collect_n_ingest` service proceeds with Spark ingestion jobs.
      * `spark_ingest/text_ingest.py` reads raw textual data from `data/raw`, transforms it, and writes it to the `delta_lake/text_table` Delta Lake table.
      * `spark_ingest/threat_mapping_ingest.py` reads `config/threat_mapping.json` and ingests this mapping data into the `delta_lake/threat_mapping` Delta Lake table.
      * Ingestion logs are recorded in `logs/ingestion.log`.

3.  **Enrichment:**

      * The `enrich_stream` Docker service, which depends on the `collect_n_ingest` service, begins the enrichment process.
      * `spark_enrichment/stream_text_enrichment.py` reads data from `delta_lake/text_table`.
      * It performs enrichment operations.
      * The enriched data is then written to the `delta_lake/enriched_table` Delta Lake table.
      * Enrichment logs are stored in `logs/stream_enrichment.log`.

## 4\. Schema Designs

### 4.1. `text_table` (Delta Lake)

This table stores the raw textual content collected from various sources.

| Field               | Type            | Description                                             |
| ------------------- | --------------- | ------------------------------------------------------- |
| `id`                | `string`        | Unique ID of the collected content (`source__item_id`). |
| `source`            | `string`        | Data source (e.g., `dev_to`, `reddit_com`).             |
| `author`            | `string`        | Author name (if available).                             |
| `article_url`       | `string`        | Full link to the original article.                      |
| `title`             | `string`        | Title of the content.                                   |
| `short_description` | `string`        | A brief summary or excerpt from the content.            |
| `tags`              | `array<string>` | List of tags related to the content.                    |
| `content`           | `string`        | Main body of the article/post.                          |
| `created_at_utc`    | `timestamp`     | When the content was originally published.              |

### 4.2. `threat_mapping` (Delta Lake)

This table stores mapping information, likely for identifying or categorizing threats within the text, sourced from `threat_mapping.json`.

| Field      | Type     | Description                                  |
| ---------- | -------- | -------------------------------------------- |
| `keyword`  | `string` | Keyword or phrase to be matched in the text. |
| `category` | `string` | Associated threat category.                  |
| `severity` | `string` | Severity level: `low`, `medium`, or `high`.  |

### 4.3. `enriched_table` (Delta Lake)

This table stores the textual content after enrichment, combining information from `text_table` and `threat_mapping`, potentially with additional NLP-derived features.

| Field            | Type        | Description                              |
| ---------------- | ----------- | ---------------------------------------- |
| `id`             | `string`    | Original ID from `text_table`.           |
| `content`        | `string`    | Full textual content.                    |
| `created_at_utc` | `timestamp` | When the original content was published. |
| `category`       | `string`    | Matched threat category from mapping.    |
| `keyword`        | `string`    | Matched keyword.                         |
| `severity`       | `string`    | Severity level of the matched keyword.   |

## 5\. Setup and Execution

### 5.1. Prerequisites

  * [Docker](https://docs.docker.com/get-docker/)
  * [Docker Compose](https://docs.docker.com/compose/install/)

### 5.2. Project Structure

Ensure your project directory is organized as follows:

```
activefence/
├── collectors/
│   ├── __init__.py
│   ├── dev_to.py
│   ├── nakedcapitalism_com.py
│   └── reddit_com.py
├── common/
│   ├── __init__.py
│   └── utils.py
├── config/
│   └── threat_mapping.json
├── data/  # Will be created if missing
│   └── raw/
├── delta_lake/  # Will be created if missing
│   ├── enriched_table/
│   ├── text_table/
│   └── threat_mapping/
├── logs/  # Will be created if missing
│   ├── collectors.log
│   ├── ingestion.log
│   └── stream_enrichment.log
├── spark_enrichment/
│   ├── __init__.py
│   └── stream_text_enrichment.py
├── spark_ingest/
│   ├── __init__.py
│   ├── text_ingest.py
│   └── threat_mapping_ingest.py
├── sql/
│   ├── daily_threats_count_by_category_per_source.sql
│   ├── daily_threats_ratio_vs_benign_events_per_source.sql
│   └── latest_row_per_category.sql
├── .env
├── .gitignore
├── __init__.py
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

### 5.3. Configuration

  * **`.env`**: Contains environment variables for the project:
      * `LAST_N_DAYS=5`: Defines the number of past days for data collection (if applicable).
      * `REQUEST_TIMEOUT=5`: Timeout for HTTP requests by collectors.
      * `REQUEST_RETRIES=2`: Number of retries for HTTP requests.
      * `MIN_CONTENT_LEN=50`: Minimum content length for collected text.
      * `PYSPARK_PYTHON=python3`: Python executable for PySpark.
      * `JAVA_HOME=/opt/bitnami/java`: `JAVA_HOME` path within the Docker container.
  * **`config/threat_mapping.json`**: Ensure this file is correctly formatted with your threat mapping data.

### 5.4. Building the Docker Images

Navigate to the root directory of your project (where `docker-compose.yml` is located) in your terminal:

```bash
docker compose build
```

This command will build the Docker images for both `collect_n_ingest` and `enrich_stream` services.

### 5.5. Running the Pipeline

Once the images are built, execute the pipeline using Docker Compose:

```bash
docker compose up
```

This command will:

1.  Start the `collect_n_ingest` service, executing the collectors and then the Spark ingestion jobs.
2.  Upon successful completion of `collect_n_ingest`, the `enrich_stream` service will start, running the Spark enrichment job.

To run the services in detached mode (in the background):

```bash
docker compose up -d
```

_Logs of the running services can be viewed in the `logs` directory._

To stop the services:

```bash
docker compose down
```

### 5.6. Cleaning Up Data

To remove the locally persisted data (useful for a clean run):

```bash
rm -rf data delta_lake logs
```

## 6\. Assumptions and Design Decisions

### 6.1. Core Assumptions

* Data is textual-first: The primary focus is on ingesting and analyzing structured text content from online sources (e.g., blogs, forums). Image-based threat detection (OCR) is considered out of scope for this version.

* Threat detection is rule-based: Threat identification relies on pattern matching using predefined keywords and categories from a static mapping file (threat_mapping.json). No ML/NLP classifiers are used.

* Mapping file is manually curated: The mapping of keywords to categories and severity levels is manually defined and assumed to be reasonably complete and accurate for this proof of concept.

* Streaming is append-only: New data is only appended to the text_table, and streaming enrichment assumes no late updates or deletions.

* Content is cleaned beforehand: Parsers are assumed to produce text with basic preprocessing (e.g., trimmed content, no HTML noise).

* Exact matching is sufficient: Threat keywords are matched using lowercased substring logic. No stemming, lemmatization, or semantic fuzziness is applied.

* Delta Lake paths are shared: All components (ingestion, enrichment) use a shared volume for reading/writing Delta tables, assuming atomic operations via Spark.

### 6.2. Design Decisions

  * **Dockerized Environment:** The pipeline is fully containerized for portability, isolation, and reproducibility.
  * **Modular Collectors:** Each data source has a dedicated Python script, promoting maintainability and independent development.
  * **Local Volume Mounting:** Data, Delta Lake tables, and logs are persisted to local volumes, ensuring data retention and easy access from the host machine.
  * **Sequential Execution:** The `docker compose` command orchestrates a simple sequential execution of collectors and Spark jobs.

### 6.3. Scope Limitations and Acknowledged Omissions

**Note from the Developer:**

Given the tight deadline (4 days) and existing full-time and part-time employment commitments, a pragmatic approach was taken to prioritize core functionality. This involved making deliberate choices to scope out highly complex or time-consuming features.

**Specifically, the following were intentionally omitted and are considered out of scope for this initial version due to significant time investment required:**

  * **Image Collection, Ingestion, and Enrichment:** The current pipeline exclusively handles textual content. The capability to collect, ingest, or perform any form of enrichment on data contained within images was not implemented. This includes:
      * **Handwritten Text Recognition (OCR):** Implementing a robust solution for extracting handwritten text from images would necessitate the integration of sophisticated machine learning models, often requiring extensive training data, specialized hardware (e.g., GPUs), and complex pre-processing. This alone could constitute a dedicated long-term project.

The focus remained on building a solid foundation for textual data processing, acknowledging that image-based data handling would require a considerably larger development effort than the time frame allowed.

## 7\. Future Improvements

  * **Orchestration with Apache Airflow:** For production-grade environments, the current sequential execution of collection and ingestion jobs can be greatly improved by using a dedicated workflow orchestrator like Apache Airflow. Airflow would enable:
      * **Scheduled Runs:** Define daily, hourly, or other recurring schedules for the pipeline.
      * **Dependency Management:** Explicitly define task dependencies, ensuring stages run only after their predecessors complete successfully.
      * **Retries and Error Handling:** Implement robust retry mechanisms and alert on failures.
      * **Monitoring and Logging:** Centralized monitoring of pipeline health and detailed task logs.
      * **Dynamic Task Generation:** Potentially generate collection tasks dynamically based on a list of sources.
  * **Incremental Data Loading:** Implement incremental loading for both collectors and Spark jobs to process only new or updated data, improving efficiency.
  * **Enhanced Error Handling and Retries:** More sophisticated error handling within collector and Spark scripts, including dead-letter queues for failed records.
  * **Data Quality Checks:** Implement data quality checks (e.g., schema validation, content validation) at various stages of the pipeline.
  * **Advanced NLP Enrichment:** Integrate more sophisticated NLP models for richer enrichment, such as:
      * Topic Modeling
      * Emotion Detection
      * Advanced Entity Linking
  * **Scalable Collector Framework:** Develop a more generic and scalable framework for collectors that can be easily extended to new sources without significant code changes.
  * **Image Processing Pipeline:** As noted in the scope limitations, a separate, dedicated pipeline for image collection, ingestion, and OCR (both printed and handwritten) would be a significant future enhancement, requiring substantial research and development into relevant ML models and infrastructure.
