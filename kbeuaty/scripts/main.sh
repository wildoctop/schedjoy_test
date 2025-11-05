#!/bin/bash

# --- 1. CONFIGURATION ---

# 1.1 Python Environment
# Using 'poetry run' makes PYTHON_EXEC unnecessary, as Poetry manages the path.
# Set the project root if the script is run from a different directory
# PROJECT_ROOT="/path/to/your/poetry/project"
# cd "$PROJECT_ROOT" || exit 1

# 1.2 Script Paths
PARSER_SCRIPT="scraper_kbeauty.py" # These paths are relative to the project root
EXPORT_SCRIPT="export.py"

# 1.3 Database Configuration (Passed as Environment Variables)
# NOTE: These names must match what your Python files expect (e.g., in DB_CONFIG)
export DB_NAME="parser_db"
export DB_USER="parser_user"
export DB_PASS="parSer2025!*" # !! CRITICAL: Change this! !!
export DB_HOST="localhost"

# 1.4 Logging and Error Handling
LOG_FILE="./script_log_$(date +%Y%m%d_%H%M%S).log"
"


# --- 2. FUNCTIONS ---

# Function to check the exit code of the last command
check_exit_code() {
    EXIT_CODE=$?
    STEP_NAME="$1"

    if [ $EXIT_CODE -eq 0 ]; then
        echo "SUCCESS: $STEP_NAME completed successfully." | tee -a "$LOG_FILE" 
    elif [ $EXIT_CODE -eq 2 ]; then
        echo "ERROR ($EXIT_CODE): $STEP_NAME failed due to a Database Error." | tee -a "$LOG_FILE"
        # Send specialized alert
        echo "DB Error in $STEP_NAME. Check log file $LOG_FILE" 
        exit 1 
    else
        echo "FATAL ERROR ($EXIT_CODE): $STEP_NAME failed." | tee -a "$LOG_FILE"
        # Send general failure alert
        echo "Fatal error in $STEP_NAME. Check log file $LOG_FILE"
        exit 1
    fi
}


# --- 3. MAIN EXECUTION ---

echo "--- Pipeline Start (using Poetry): $(date) ---" | tee "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"
echo "---------------------------------" | tee -a "$LOG_FILE"

# 3.1 Run parser.py using 'poetry run python'
echo "Starting Step 1: Running $PARSER_SCRIPT" | tee -a "$LOG_FILE"
# The 'poetry run python' command executes the script within the venv
poetry run python "$PARSER_SCRIPT" 2>&1 | tee -a "$LOG_FILE"
check_exit_code "$PARSER_SCRIPT"

# 3.2 Run export.py using 'poetry run python'
echo "Starting Step 2: Running $EXPORT_SCRIPT" | tee -a "$LOG_FILE"
poetry run python "$EXPORT_SCRIPT" 2>&1 | tee -a "$LOG_FILE"
check_exit_code "$EXPORT_SCRIPT"

# --- 4. CONCLUSION ---

echo "---------------------------------" | tee -a "$LOG_FILE"
echo "Pipeline finished successfully: $(date)" | tee -a "$LOG_FILE"
exit 0