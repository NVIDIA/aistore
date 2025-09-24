# AIStore Batch Get Performance Testing

This directory contains performance testing tools to compare AIStore's batch get operations against sequential object retrieval, with comprehensive analysis across different object sizes and batch configurations.

## Overview

The performance test suite consists of two main components:

1. **`populate_bucket.py`** - Creates test objects in an AIStore bucket
2. **`get_batch_bench.py`** - Benchmarks batch vs sequential get operations

### What This Test Measures

The benchmark compares:
- **Sequential Gets**: Retrieving objects one by one in sequence
- **Batch Gets**: Retrieving multiple objects using AIStore's batch API

Key metrics measured:
- Total execution time
- Throughput (objects/second)
- Data throughput (MiB/second)
- Success/failure rates
- Performance improvement ratios

## Prerequisites

1. AIStore cluster running and accessible
2. Python AIStore SDK installed
3. Required packages: `tqdm`, `aistore`

## Step 1: Create Test Objects

Before running benchmarks, populate a bucket with test objects using `populate_bucket.py`.

### Basic Usage

```bash
python populate_bucket.py
```

### Configuration Options

Edit the configuration section in `populate_bucket.py`:

```python
# Configuration parameters
BUCKET_NAME = "benchmark-100KiB"    # Bucket name
NUM_OBJECTS = 100000                # Number of objects to create
OBJECT_SIZE = 100 * 1024           # Object size in bytes (100 KiB)
MAX_WORKERS = 120                   # Concurrent upload threads
AISTORE_URL = "http://<ais-endpt>:51080"  # AIStore endpoint
```

### Testing Different Object Sizes

Create buckets with different object sizes by adjusting `OBJECT_SIZE`:

```python
# Examples for different sizes
OBJECT_SIZE = 1 * 1024      # 1 KiB objects
OBJECT_SIZE = 10 * 1024     # 10 KiB objects  
OBJECT_SIZE = 100 * 1024    # 100 KiB objects
OBJECT_SIZE = 1024 * 1024   # 1 MiB objects
OBJECT_SIZE = 10 * 1024 * 1024  # 10 MiB objects
```

**Naming Convention**: Use descriptive bucket names like:
- `benchmark-1KiB`
- `benchmark-10KiB` 
- `benchmark-100KiB`
- `benchmark-1MiB`

## Step 2: Run Performance Benchmarks

After creating test objects, run the benchmark suite with `get_batch_bench.py`.

### Basic Usage

```bash
python get_batch_bench.py
```

### Configuration Options

Edit the configuration section in `get_batch_bench.py`:

```python
# Configuration
AISTORE_URL = "http://<ais-endpt>:51080"
BUCKET_NAME = "benchmark-10KiB"     # Match your populated bucket
TOTAL_OBJECTS = 10000               # Objects to test (≤ populated amount)
```

### Batch Test Configurations

The benchmark runs these test combinations:

| Batch Size | Num Batches | Total Objects | Streaming |
|------------|-------------|---------------|-----------|
| 100        | 100         | 10,000        | True/False |
| 1,000      | 10          | 10,000        | True/False |
| 10,000     | 1           | 10,000        | True/False |

### Customizing Test Configurations

Modify the `test_configs` list in `run_comprehensive_benchmark()`:

```python
test_configs = [
    # (batch_size, num_batches, streaming)
    (50, 200, True),        # Custom: 50 objects per batch
    (2000, 5, False),       # Custom: 2000 objects per batch
    (5000, 2, True),        # Custom: 5000 objects per batch
]
```

**Note**: Ensure `batch_size * num_batches ≤ TOTAL_OBJECTS`

## Understanding Results

### Console Output

The benchmark provides detailed output including:

1. **Baseline (Sequential) Results**:
   - Duration, throughput, success rate

2. **Batch Results Comparison**:
   - Side-by-side performance metrics
   - Speedup factors and improvement ratios

3. **Detailed Analysis**:
   - Per-configuration breakdown
   - Statistical comparisons

### JSON Output

Results are automatically saved to timestamped JSON files:
```
benchmark_results_YYYYMMDD_HHMMSS.json
```

## Example Workflow

```bash
# 1. Create 10,000 objects of 100 KiB each
# Edit populate_bucket.py configuration:
# BUCKET_NAME = "benchmark-100KiB"
# NUM_OBJECTS = 10000
# OBJECT_SIZE = 100 * 1024
python populate_bucket.py

# 2. Run comprehensive benchmark
# Edit get_batch_bench.py configuration:
# BUCKET_NAME = "benchmark-100KiB"
# TOTAL_OBJECTS = 10000
python get_batch_bench.py

# 3. Review results in console output and JSON file
```
### Performance Tuning

- Adjust `max_pool_size` in Client initialization
- Modify `MAX_WORKERS` in populate_bucket.py
- Test different batch sizes for your specific use case
- Consider object size vs batch size trade-offs

## Results Interpretation

- **Speedup Factor > 1**: Batch operation is faster than sequential
- **Speedup Factor < 1**: Sequential operation is faster than batch
- **Throughput Improvement**: Ratio of batch vs sequential throughput
- **Success Rate**: Percentage of successful object retrievals
