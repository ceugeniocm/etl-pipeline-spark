# Requirements

### Overview & Goals
The goal is to implement a visual progress indicator in the ETL pipeline to provide real-time feedback on the execution status of the various stages. This will be achieved using the `tqdm` library.

### Scope
- **In Scope**:
    - Adding `tqdm` to `requirements.txt`.
    - Integrating a progress bar into the `run_pipeline` function in `etl_spark.py`.
    - Updating stage descriptions to be more user-friendly.
- **Out of Scope**:
    - Implementing progress bars for internal Spark transformations (relying on stage-level progress).

# Technical Design

### Current Implementation
- The pipeline logs progress messages using the `logging` module.
- There is no visual representation of the overall completion percentage.

### Proposed Changes
1.  **Dependencies**:
    - Add `tqdm>=4.66.0` to `requirements.txt`.

2.  **Application Logic (`etl_spark.py`)**:
    - Import `tqdm`.
    - Initialize a `tqdm` progress bar with 5 steps (the main stages of the ETL).
    - Update the progress bar after each major step is completed.
    - Ensure `logging` output doesn't interfere with the progress bar (using `tqdm` context if possible or simply updating after logging).

### File Structure
- `requirements.txt`: Modified to include `tqdm`.
- `etl_spark.py`: Modified to import and use `tqdm`.

# Testing

### Validation Approach
- **Installation Check**: Ensure `tqdm` can be installed.
- **Visual Verification**: Run the pipeline (if possible with a small dataset) and observe the progress bar.
- **Log Consistency**: Ensure that logs are still captured correctly in `etl.log` and displayed in the console alongside the progress bar.

# Delivery Steps

### ✓ Step 1: Add tqdm to requirements.txt
Add `tqdm>=4.66.0` to the dependency list.

### ✓ Step 2: Implement progress indicator in etl_spark.py
- Import `tqdm`.
- Wrap the main pipeline logic with a `tqdm` context manager.
- Update the progress bar at each step.
