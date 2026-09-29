# Local Spark filesystem adapter for Windows

`hadoop-bare-naked-local-fs-0.1.0.jar` is the Apache-2.0-licensed GlobalMentor local Hadoop filesystem adapter, downloaded from Maven Central:

`com.globalmentor:hadoop-bare-naked-local-fs:0.1.0`

PySpark 4.0.1 can read the CSVs on Windows without this adapter, but its bundled Hadoop 3.4.1 local filesystem cannot create Parquet output directories without `winutils.exe`. This small Java adapter routes local file operations through the Java filesystem API. `src/eda.py` adds it to the existing `.venv` PySpark JAR directory and configures it before starting Spark. It does not install or run Hadoop services. Hadoop may still print a startup warning that `winutils.exe` is absent; the adapter makes local Parquet writes/reads succeed despite that warning.

The JAR was smoke-tested with the project's PySpark 4.0.1 / Hadoop 3.4.1 runtime for a local Parquet write-and-read. SHA-256: `E0CC30FB0531EB0B59468DC0ABF5B257533D2365B5E9F45E795EDD707AA78C62`.
