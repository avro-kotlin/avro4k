# Kotlin Avro Benchmark

This project contains a benchmark that compares the serialization / deserialization performance of the following avro libraries:

- [Avro4k](https://github.com/avro-kotlin/avro4k/)
- [Jackson Avro](https://github.com/FasterXML/jackson-dataformats-binary/tree/master/avro)
- Coming soon: [Avro](https://avro.apache.org/)

## Results

<details>
<summary>Macbook air M2</summary>

```
Benchmark                           Mode  Cnt       Score   Error  Units
Avro4kClientsBenchmark.read        thrpt    2  439983.130          ops/s
Avro4kClientsBenchmark.write       thrpt    2  474453.236          ops/s
JacksonAvroClientsBenchmark.read   thrpt    2  577757.798          ops/s
JacksonAvroClientsBenchmark.write  thrpt    2  649982.820          ops/s
```

For the moment, Jackson Avro is faster than Avro4k because Avro4k is still not doing direct encoding so there is an intermediate generic data step.

</details>

## Run the benchmark locally

Just execute the benchmark:

```shell
../gradlew benchmark
```

You can get the results in the `build/reports/benchmarks/main` directory.

## Other information

Thanks for [@twinprime](https://github.com/twinprime) for this initiative.
