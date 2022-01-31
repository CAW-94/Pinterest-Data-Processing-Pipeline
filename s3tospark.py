import findspark
import multiprocessing
import pyspark


findspark.init()

cfg = (
    pyspark.SparkConf()
    # Setting where master node is located [cores for multiprocessing]
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("PinterestTestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
)

# Getting a single variable
print(cfg.get("spark.executor.memory"))
# Listing all of them in string readable format
print(cfg.toDebugString())


