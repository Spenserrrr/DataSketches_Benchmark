package benchmark

import benchmark.data.DatasetLoader
import benchmark.materialization.MaterializationBenchmark
import benchmark.sketch.{PartitionedSketches, SketchFunctions}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BenchmarkRunner {
  private final case class ApproxMethod(
      name: String,
      run: (SparkSession, DataFrame, QuerySpec) => (DataFrame, Long),
      relativeSd: Option[Double],
      notes: String
  )

  def main(args: Array[String]): Unit = {
    val config = BenchmarkConfig.parse(args)
    val localParallelism = math.max(config.partitions, 1)

    val spark = SparkSession
      .builder()
      .appName("DataSketches Benchmark Baseline")
      .master(s"local[$localParallelism]")
      .config("spark.sql.shuffle.partitions", localParallelism.toString)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    SketchFunctions.register(spark, config.thetaLgK, config.hllLgK)

    try {
      val runDirectory = ResultWriter.createRunDirectory(config.outputRoot)
      val input = DatasetLoader.loadAndMaterialize(spark, config, runDirectory).cache()
      val inputRows = input.count()
      input.createOrReplaceTempView(DatasetLoader.ViewName)

      val results = BenchmarkQueries.AllQueries.flatMap { query =>
        runQueryPair(spark, config, input, query, inputRows)
      }

      val outputPath = ResultWriter.writeResults(results, runDirectory)
      println(s"Wrote ${results.size} benchmark rows to $outputPath")

      val materializationOutputPath = MaterializationBenchmark.run(
        spark = spark,
        config = config,
        input = input,
        inputRows = inputRows,
        exactQueries = exactQueries(spark),
        queries = BenchmarkQueries.AllQueries,
        runDirectory = runDirectory
      )
      println(s"Wrote materialization results to $materializationOutputPath")
    } finally {
      spark.stop()
    }
  }

  private def runQueryPair(
      spark: SparkSession,
      config: BenchmarkConfig,
      input: DataFrame,
      query: QuerySpec,
      inputRows: Long
  ): Seq[BenchmarkResult] = {
    println(s"Running ${query.name}")

    val (exactDf, exactRuntimeMs) =
      timedQuery(spark, query.exactSql(DatasetLoader.ViewName))

    val exactResult = Metrics.exactResult(config.dataset, query, exactDf, inputRows, exactRuntimeMs)
    val approximateResults = approximateMethods(config).map { method =>
      val (approxDf, approxRuntimeMs) = method.run(spark, input, query)
      Metrics.approximateResult(
        spark = spark,
        dataset = config.dataset,
        query = query,
        method = method.name,
        exactDf = exactDf,
        approxDf = approxDf,
        inputRows = inputRows,
        runtimeMs = approxRuntimeMs,
        relativeSd = method.relativeSd,
        notes = method.notes
      )
    }

    exactResult +: approximateResults
  }

  private def approximateMethods(config: BenchmarkConfig): Seq[ApproxMethod] = {
    val directUdafMethods =
      if (config.includeDirectUdaf) {
        Seq(
          ApproxMethod(
            name = "datasketches_theta_udaf",
            run = (spark, _, query) => timedQuery(spark, query.sketchSql(DatasetLoader.ViewName, SketchFunctions.ThetaFunctionName)),
            relativeSd = None,
            notes = s"Apache DataSketches Theta direct UDAF; lgK=${config.thetaLgK}"
          ),
          ApproxMethod(
            name = "datasketches_hll_udaf",
            run = (spark, _, query) => timedQuery(spark, query.sketchSql(DatasetLoader.ViewName, SketchFunctions.HllFunctionName)),
            relativeSd = None,
            notes = s"Apache DataSketches HLL direct UDAF; lgK=${config.hllLgK}"
          )
        )
      } else {
        Seq.empty
      }

    Seq(
      ApproxMethod(
        name = "spark_approx_count_distinct",
        run = (spark, _, query) => timedQuery(spark, query.approxSql(DatasetLoader.ViewName, config.relativeSd)),
        relativeSd = Some(config.relativeSd),
        notes = "Spark built-in HLL++ approximation"
      ),
      ApproxMethod(
        name = "datasketches_theta_partitioned",
        run = (spark, input, query) =>
          timedDataFrame(PartitionedSketches.estimate(spark, input, query, PartitionedSketches.Theta, config.thetaLgK)),
        relativeSd = None,
        notes = s"Apache DataSketches Theta partition-level sketching; lgK=${config.thetaLgK}"
      ),
      ApproxMethod(
        name = "datasketches_hll_partitioned",
        run = (spark, input, query) =>
          timedDataFrame(PartitionedSketches.estimate(spark, input, query, PartitionedSketches.Hll, config.hllLgK)),
        relativeSd = None,
        notes = s"Apache DataSketches HLL partition-level sketching; lgK=${config.hllLgK}"
      )
    ) ++ directUdafMethods
  }

  private def exactQueries(spark: SparkSession): Map[String, (DataFrame, Long)] =
    BenchmarkQueries.AllQueries.map { query =>
      query.name -> timedQuery(spark, query.exactSql(DatasetLoader.ViewName))
    }.toMap

  private def timedQuery(spark: SparkSession, sqlText: String): (DataFrame, Long) = {
    timedDataFrame(spark.sql(sqlText))
  }

  private def timedDataFrame(df: => DataFrame): (DataFrame, Long) = {
    val startNs = System.nanoTime()
    val computed = df.cache()
    computed.count()
    val elapsedMs = (System.nanoTime() - startNs) / 1000000L
    (computed, elapsedMs)
  }
}
