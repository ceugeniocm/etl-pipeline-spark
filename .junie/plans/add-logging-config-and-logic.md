---
sessionId: session-260804-173514-1lyj
---

# Requirements

### Overview & Goals
The goal is to implement a logging configuration in the ETL pipeline to allow control over the verbosity and output destination of logs. This involves adding the configuration to the `config_bigdata.json` file and updating the `etl_spark.py` script to respect these settings.

### Scope
- **In Scope**:
    - Adding a `run` configuration section to `config_bigdata.json`.
    - Implementing a logging setup in `etl_spark.py` using Python's `logging` module.
    - Configuring Spark's internal log level to match the user's preference (e.g., `ERROR`).
    - Redirecting log output to a file (`etl.log`) when configured.
- **Out of Scope**:
    - Changing the logic of the ETL transformations themselves.
    - Implementing log rotation (can be added later if needed).

# Technical Design

### Current Implementation
- `etl_spark.py` currently uses `print()` for all status messages.
- Spark's internal logging level is not explicitly set, often resulting in verbose `INFO` logs from Spark's core and libraries.
- Configurations are loaded from `config_bigdata.json` but lack a section for runtime execution parameters.

### Proposed Changes
1.  **Configuration**:
    - Add a `run` section to `config_bigdata.json` at the root level.
    ```json
    "run": {
      "log_level": "ERROR",
      "log_file": "etl.log"
    }
    ```

2.  **Application Logic (`etl_spark.py`)**:
    - **Initialization**: Add a `setup_logging` function that configures `logging.basicConfig` with both a `StreamHandler` (console) and a `FileHandler` (using `log_file` from config).
    - **Spark Integration**: Update `create_spark_session` to call `spark.sparkContext.setLogLevel(config['run']['log_level'])` after the session is created.
    - **Log Messages**: Transition key progress messages from `print()` to `logging.info()` or `logging.error()` to ensure they are captured in the log file and respect the `log_level`.

### File Structure
- `config_bigdata.json`: Modified to include the `run` section.
- `etl_spark.py`: Modified to import `logging` and implement the setup logic.

# Testing

### Validation Approach
- **Configuration Check**: Verify that `config_bigdata.json` contains the new keys.
- **Log File Creation**: Run the pipeline and ensure `etl.log` is created in the project root.
- **Log Verbosity**: Verify that Spark `INFO` messages are no longer displayed in the console when `log_level` is set to `ERROR`.
- **Log Content**: Check that `etl.log` contains the expected pipeline progress messages.

# Delivery Steps

### ✓ Step 1: Add logging configuration to config_bigdata.json
Add the `run` block to the root of `config_bigdata.json`.
- Key: `"run"`
- Values: `{"log_level": "ERROR", "log_file": "etl.log"}`

### ✓ Step 2: Implement logging logic in etl_spark.py
Enable runtime logging based on the new configuration.
- Import the `logging` module.
- Implement a `setup_logging` function to initialize Python logging (console + file).
- Call `setup_logging` in `run_pipeline` after loading the configuration.
- Update `create_spark_session` to set the Spark internal log level using `spark.sparkContext.setLogLevel()`.
- Replace primary status `print()` calls in `run_pipeline` and `extract` with appropriate `logging` calls.