# Benchmarks for Minimal AIS Deployment on EC2 Instance

## Write Benchmarks

Benchmarks for write speeds -- with varying object sizes -- were performed with the following `aisloader` command:

```
aisloader -bucket=ais://write-benchmark-<obj-size> -cleanup=false -totalputsize=30G -duration=0 -minsize=<obj-size> -maxsize=<obj-size> -numworkers=8 -pctput=100 -cleanup=false
```

```
128K:
Time      OP  Count  Size (Total)   Latency (min, avg, max)   Throughput (Avg)  Errors (Total)
22:53:09  PUT 251,405 30.7GiB     595.237µs 6.989ms 309.024ms 142.83MiB/s       0

1M:
Time      OP  Count  Size (Total)   Latency (min, avg, max)   Throughput (Avg)  Errors (Total)
22:18:25  PUT 30,784 30.1GiB      2.570ms 54.569ms 204.060ms  146.55MiB/s       0

8M:
Time      OP  Count  Size (Total)   Latency (min, avg, max)    Throughput (Avg)  Errors (Total)
22:04:17  PUT 3,877  30.3GiB     15.777ms 433.934ms  731.998ms 147.33MiB/s       0
```

## Read

Benchmarks for read speeds -- with varying object sizes -- were performed with the following `aisloader` command:
```
aisloader -bucket=ais://benchmark-<obj-size> -duration=10m -minsize=<obj-size> -maxsize=<obj-size> -cleanup=false -numworkers=8
```  

```
128K:
Time      OP  Count     Size (Total)   Latency (min, avg, max)    Throughput (Avg)  Errors (Total)
02:41:03  GET 1,403,742 171.4GiB    215.148µs  3.414ms  118.008ms 292.44MiB/s       0

1M:
Time      OP  Count   Size (Total)   Latency (min, avg, max)    Throughput (Avg)  Errors (Total)
02:07:19  GET 195,965 191.4GiB    629.478µs  24.488ms 116.694ms 326.60MiB/s       0 

8M:
Time      OP  Count   Size (Total)   Latency (min, avg, max)    Throughput (Avg)  Errors (Total)
01:48:25  GET 26,070  203.7GiB     3.453ms  184.141ms  1.018s   347.52MiB/s       0
```
