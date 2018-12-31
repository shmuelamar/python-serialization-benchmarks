# Python Serialization Benchmarks

## Benchmarked Libraries

* bson v3.7.2 (pymongo version)
* cbor v1.0.0
* json v2.0.9 (python v3.7.1)
* msgpack v0.5.6
* parquet v0.11.1 (pyarrow version)
* pickle v3.7.1 (python version)
* protobuf v3.6.1
* ujson v1.35


## Results

![serialization bench](results/avg_serde_time_1m_objects.png)

![serialization bench](results/ser_time_single_object.png)

![serialization bench](results/avg_ser_time_1m_objects.png)

![serialization bench](results/avg_size_1m_objects.png)


detailed results can be found on `results/` dir

**Machine Info:** Linux 64bit, CPython 3.7.1 build: GCC 7.3.0 default-Dec 14 2018 19:28:38

## Running Benchmarks Locally

**requirements:**
* miniconda
* make


first lets setup miniconda env with the requirements:

```bash
$ make setup-env
```

run the benchmarks:

```bash
$ make bench
```

the above will create two files - `results-summary.csv` and `detailed-results.json`
