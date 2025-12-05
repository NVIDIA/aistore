#!/bin/bash

set -euo pipefail

# ==============================================================================
# AIStore Batch GET Benchmark Testing Script
# ==============================================================================
# This script runs comprehensive benchmark tests for different object sizes
# and batch sizes to evaluate AIStore GET performance.
#
# Object Sizes: 10KiB, 100KiB, 1MiB, 10MiB
# Batch Sizes: 16, 32, 64
# Duration: 1 hour per test
# ==============================================================================

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print timestamped messages
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

log_info() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1"
}

# Function to print section headers
print_header() {
    echo ""
    echo -e "${BLUE}================================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================================================${NC}"
    echo ""
}

# Function to check if required files exist
check_prerequisites() {
    log "Checking prerequisites..."
    
    local required_scripts=("clear_pagecache.sh" "run_get_bench.sh" "run_get_batch_bench.sh")
    local missing_scripts=()
    
    for script in "${required_scripts[@]}"; do
        if [[ ! -f "./$script" ]]; then
            missing_scripts+=("$script")
        fi
    done
    
    if [[ ${#missing_scripts[@]} -gt 0 ]]; then
        log_error "Missing required scripts: ${missing_scripts[*]}"
        exit 1
    fi
    
    log "✓ All required scripts found"
}

# Configuration
export AISLOADER_DURATION="1h"
BATCH_SIZES=(16 32 64)
BASE_LISTS_DIR="/Users/abhgaikwad/lists"

# Define benchmark configurations
# Format: "object_size|bucket_name|list_file"
BENCHMARKS=(
    "10KiB|ais://ais-bench-10KiB|${BASE_LISTS_DIR}/ais-bench-10KiB.txt"
    "100KiB|ais://ais-bench-100KiB|${BASE_LISTS_DIR}/ais-bench-100KiB.txt"
    "1MiB|ais://ais-bench-1MiB|${BASE_LISTS_DIR}/ais-bench-1MiB.txt"
    "10MiB|ais://ais-bench-10mb|${BASE_LISTS_DIR}/ais_bench_10mb_cleaned.txt"
)

# Counters for summary
TOTAL_TESTS=0
SUCCESSFUL_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

# Start time
START_TIME=$(date +%s)

# Main execution
print_header "AIStore Batch GET Benchmark Testing"
log_info "Test Duration per benchmark: ${AISLOADER_DURATION}"
log_info "Batch sizes to test: ${BATCH_SIZES[*]}"
log_info "Object sizes to test: 10KiB, 100KiB, 1MiB, 10MiB"
log_info "Total benchmarks to run: $((${#BENCHMARKS[@]} * 4)) (${#BENCHMARKS[@]} object sizes × 4 tests each)"
echo ""

check_prerequisites

# Iterate through each object size configuration
for BENCHMARK in "${BENCHMARKS[@]}"; do
    # Parse the benchmark configuration
    IFS='|' read -r OBJ_SIZE BUCKET_NAME LIST_FILE <<< "$BENCHMARK"
    
    print_header "Testing Object Size: ${OBJ_SIZE}"
    
    # Validate list file exists
    # Commented out as file-lists were copied to the hosts from AIS
    # if [[ ! -f "$LIST_FILE" ]]; then
    #     log_error "List file not found: ${LIST_FILE}"
    #     log_warning "Skipping ${OBJ_SIZE} tests"
    #     SKIPPED_TESTS=$((SKIPPED_TESTS + 4))  # 1 regular + 3 batch tests
    #     continue
    # fi
    
    export AISLOADER_BUCKET="$BUCKET_NAME"
    export AISLOADER_OBJECTS="$LIST_FILE"
    
    log_info "Bucket: ${AISLOADER_BUCKET}"
    log_info "Objects list: ${AISLOADER_OBJECTS}"
    echo ""
    
    # Run standard GET benchmark (no batching)
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    log "Starting standard GET benchmark for ${OBJ_SIZE}..."
    
    if ./clear_pagecache.sh; then
        log "✓ Page cache cleared"
    else
        log_warning "Failed to clear page cache (may require sudo)"
    fi
    
    if ./run_get_bench.sh; then
        log "✓ Standard GET benchmark completed for ${OBJ_SIZE}"
        SUCCESSFUL_TESTS=$((SUCCESSFUL_TESTS + 1))
        
        # Consolidate results
        BUCKET_DIR=$(basename "$BUCKET_NAME" | sed 's|ais://||')
        RESULTS_DIR="output/get/${BUCKET_DIR}"
        if [[ -d "$RESULTS_DIR" ]]; then
            log_info "Consolidating results for standard GET..."
            echo ""
            python3 consolidate_results.py "$RESULTS_DIR" get
            echo ""
        else
            log_warning "Results directory not found: ${RESULTS_DIR}"
        fi
    else
        log_error "Standard GET benchmark failed for ${OBJ_SIZE}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
    
    # Run batch GET benchmarks
    for BATCH_SIZE in "${BATCH_SIZES[@]}"; do
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        export AISLOADER_GET_BATCHSIZE="$BATCH_SIZE"
        
        log "Starting batch GET benchmark for ${OBJ_SIZE} with batch size ${BATCH_SIZE}..."
        
        if ./clear_pagecache.sh; then
            log "✓ Page cache cleared"
        else
            log_warning "Failed to clear page cache"
        fi
        
        if ./run_get_batch_bench.sh; then
            log "✓ Batch GET benchmark completed for ${OBJ_SIZE} (batch size: ${BATCH_SIZE})"
            SUCCESSFUL_TESTS=$((SUCCESSFUL_TESTS + 1))
            
            # Consolidate results
            BUCKET_DIR=$(basename "$BUCKET_NAME" | sed 's|ais://||')
            RESULTS_DIR="output/get_batch/${BUCKET_DIR}/${BATCH_SIZE}"
            if [[ -d "$RESULTS_DIR" ]]; then
                log_info "Consolidating results for batch GET (batch size: ${BATCH_SIZE})..."
                echo ""
                python3 consolidate_results.py "$RESULTS_DIR" get_batch
                echo ""
            else
                log_warning "Results directory not found: ${RESULTS_DIR}"
            fi
        else
            log_error "Batch GET benchmark failed for ${OBJ_SIZE} (batch size: ${BATCH_SIZE})"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
        echo ""
    done
done

# Calculate execution time
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))
EXECUTION_HOURS=$((EXECUTION_TIME / 3600))
EXECUTION_MINUTES=$(((EXECUTION_TIME % 3600) / 60))
EXECUTION_SECONDS=$((EXECUTION_TIME % 60))

# Print summary
print_header "Benchmark Testing Summary"
log_info "Total tests run: ${TOTAL_TESTS}"
log_info "Successful: ${GREEN}${SUCCESSFUL_TESTS}${NC}"
if [[ $FAILED_TESTS -gt 0 ]]; then
    log_info "Failed: ${RED}${FAILED_TESTS}${NC}"
else
    log_info "Failed: ${FAILED_TESTS}"
fi
if [[ $SKIPPED_TESTS -gt 0 ]]; then
    log_info "Skipped: ${YELLOW}${SKIPPED_TESTS}${NC}"
fi
log_info "Total execution time: ${EXECUTION_HOURS}h ${EXECUTION_MINUTES}m ${EXECUTION_SECONDS}s"
echo ""

if [[ $FAILED_TESTS -eq 0 ]] && [[ $SKIPPED_TESTS -eq 0 ]]; then
    log "${GREEN}✓ All benchmarks completed successfully!${NC}"
    exit 0
elif [[ $FAILED_TESTS -gt 0 ]]; then
    log_error "Some benchmarks failed. Please review the logs above."
    exit 1
else
    log_warning "Some benchmarks were skipped due to missing files."
    exit 0
fi
