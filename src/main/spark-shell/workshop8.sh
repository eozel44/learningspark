Spark configurations to tweakfor I/O during map and shuffle operations

spark.driver.memory

Default is 1g (1 GB). This is the amount of memory allocated to the Spark
driver to receive data from executors. This is often changed during spark-
submit with --driver-memory.
Only change this if you expect the driver to receive large amounts of data
back from operations like collect(), or if you run out of driver memory.

spark.shuffle.file.buffer

Default is 32 KB. Recommended is 1 MB. This allows Spark to do more
buffering before writing final map results to disk.

spark.file.transferTo

Default is true. Setting it to false will force Spark to use the file buffer to
transfer files before finally writing to disk; this will decrease the I/O activity.

spark.shuffle.unsafe.file.output.buffer

Default is 32 KB. This controls the amount of buffering possible when
merging files during shuffle operations. In general, large values (e.g., 1 MB)
are more appropriate for larger workloads, whereas the default can work for
smaller workloads.

spark.io.compression.lz4.blockSize

Default is 32 KB. Increase to 512 KB. You can decrease the size of the shuffle
file by increasing the compressed size of the block.

spark.shuffle.service.index.cache.size

Default is 100m. Cache entries are limited to the specified memory footprint
in byte.

spark.shuffle.registration.timeout

Default is 5000 ms. Increase to 120000 ms.

spark.shuffle.registration.maxAttempts

Default is 3. Increase to 5 if needed.



spark2-submit --master yarn --deploy-mode client
              --executor-memory 12G
              --executor-cores 4
              --driver-memory 3G
              --conf spark.executor.memoryOverhead=3072
              --conf spark.driver.memoryOverhead=4096
              --conf spark.dynamicAllocation.maxExecutors=50
              --conf spark.sql.orc.impl=native
              --conf spark.sql.orc.enableVectorizedReader=true
              --conf spark.sql.hive.convertMetastoreOrc=true
              --conf spark.executor.extraJavaOptions="-XX:ParallelGCThreads=4 -XX:+UseParallelGC"
              --conf spark.shuffle.file.buffer=1m
              --conf spark.unsafe.sorter.spill.reader.buffer.size=1m
              --conf spark.file.transferTo=false
              --conf spark.shuffle.unsafe.file.ouput.buffer=5m
              --conf spark.sql.shuffle.partitions=96
              --conf spark.default.parallelism=96
              --conf spark.io.compression.lz4.blockSize=512k
              --conf spark.shuffle.service.index.cache.size=2048
              --conf spark.sql.cbo.enabled=true
              --conf spark.hadoop.hive.ignore.mapjoin.hint=false
              --conf spark.sql.orc.filterPushdown=true
              --conf spark.sql.orc.char.enabled=true
              --conf spark.port.maxRetries=200