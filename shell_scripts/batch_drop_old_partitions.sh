#! /bin/bash

#
# Script Name:        batch_drop_old_partitions.sh
#
# Description:        This script:
#                     . Truncate previous days staging data
#                     . Executes partition management drop
#
# Usage:              batch_drop_old_partitions.sh
#

set -e

. ${ENV_BASE_DIR}/env/${env}/etl/env.properties
. `which util_file.sh`

LOG_FILE=$LOG_DIR/batch_drop_old_partitions$(date +%Y%m%d).log
ORA_LOG_FILE_TO_CHECK=${LOG_FILE}
LOG_DATETIME=$(date +%Y%m%d%H%M)
LOG_DATE=$(date +%Y%m%d)

# Check the number of parameters passed
if [[ $# -gt 1 ]]; then
   log_error "ERROR: Invalid number of parameters passed. Usage: batch_drop_old_partitions.sh" ${LOG_FILE}
fi

# Check if any batch process is currently running
check_batch_status "Batch currently running or batch failed.  Please wait for batch to finish or update UDAP_BATCH_DATE to match ETL_BATCH_DATE"

cd ${BASE_DIR}/etl

log_message "Starting batch_drop_old_partitions.sh" ${LOG_FILE}

#####################################
# Truncate Previous Days Staging Data
#####################################
log_message "Truncating Staging Data" ${LOG_FILE}
truncate_STG_data ${LOG_FILE}

# ###################################################
# Execute Partition Management to drop old partitions
# ###################################################
log_message "Execute Partition Management to drop old partitions" ${LOG_FILE}
execute_partition_management STG DROP
execute_partition_management RDS DROP
execute_partition_management IDS DROP
execute_partition_management DDS DROP
execute_partition_management BAE DROP

log_message "Batch Drop old partitions Done" ${LOG_FILE}

exit 0
