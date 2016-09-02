This is yet another couchbase query benchmark using couchbase-net-client. 

The benchmark is mainly a port of ycsb in java (https://github.com/couchbaselabs/ycsb)
It currently measures throughput and latencies for n1ql based CRUD operations using the couchbase-net-client sdk.

Options supported are 

```
netquerybench.exe /?

Usage:
        -t[optional]... Number of threads

        -d[optional]... Number of documents to be inserted in couchbase

        -o[optional]... Number of operations. Run mode will run operations up to this count

        -f[optional]... Number to fields inside json document

        -l[optional]... Json doc field length

        -w[optional]... Scan query limit

        -r[optional]... Read proportion

        -u[optional]... Update proportion

        -s[optional]... Scan proportion

        -i[optional]... Insert proportion

        -m[optional]... Mode Load/Run

        -k[optional]... Not supported yet

        -a[optional]... Read all or subset of fields

        -c[optional]... Couchbase server host name

        -p[optional]... Couchbase server custom port to connect to

        -b[optional]... Couchbase bucket name

        -g[optional]... Couchbase bucket password

```
Sample output

```
Overall Summary
Total Execution time 3746.6480653s
Throughput ops/s 266.905239715897
There were 100% of ops successful
Summary for SCAN
Operations: 1000000
AverageLatency(ms): 14.9030195082993
Min Latency(ms): 2.9098
Max Latency(ms): 255.1373
95th percentile latency(ms): 35.5645
```


