package benchmark

import java.nio.file.Path

import benchmark.sketch.SketchFunctions
import org.apache.spark.sql.{DataFrame, SparkSession}

object BenchmarkRunner {
  private final case class ApproxMethod(
      name: String,
      sql: QuerySpec => String,
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
      val input = materializeSyntheticData(spark, config, runDirectory).cache()
      val inputRows = input.count()
      input.createOrReplaceTempView(SyntheticData.ViewName)

      val results = BenchmarkQueries.BaselineQueries.flatMap { query =>
        runQueryPair(spark, config, query, inputRows)
      }

      val outputPath = ResultWriter.writeResults(results, runDirectory)
      println(s"Wrote ${results.size} benchmark rows to $outputPath")
    } finally {
      spark.stop()
    }
  }

  private def materializeSyntheticData(
      spark: SparkSession,
      config: BenchmarkConfig,
      runDirectory: Path
  ): DataFrame = {
    val syntheticPath = runDirectory.resolve("data").resolve("synthetic.parquet").toString
    SyntheticData
      .build(spark, config)
      .write
      .mode("overwrite")
      .parquet(syntheticPath)
    spark.read.parquet(syntheticPath)
  }

  private def runQueryPair(
      spark: SparkSession,
      config: BenchmarkConfig,
      query: QuerySpec,
      inputRows: Long
  ): Seq[BenchmarkResult] = {
    println(s"Running ${query.name}")

    val (exactDf, exactRuntimeMs) =
      timedQuery(spark, query.exactSql(SyntheticData.ViewName))

    val exactResult = Metrics.exactResult(query, exactDf, inputRows, exactRuntimeMs)
    val approximateResults = approximateMethods(config).map { method =>
      val (approxDf, approxRuntimeMs) = timedQuery(spark, method.sql(query))
      Metrics.approximateResult(
        spark = spark,
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

  private def approximateMethods(config: BenchmarkConfig): Seq[ApproxMethod] =
    Seq(
      ApproxMethod(
        name = "spark_approx_count_distinct",
        sql = _.approxSql(SyntheticData.ViewName, config.relativeSd),
        relativeSd = Some(config.relativeSd),
        notes = "Spark built-in HLL++ approximation"
      ),
      ApproxMethod(
        name = "datasketches_theta",
        sql = _.sketchSql(SyntheticData.ViewName, SketchFunctions.ThetaFunctionName),
        relativeSd = None,
        notes = s"Apache DataSketches Theta sketch; lgK=${config.thetaLgK}"
      ),
      ApproxMethod(
        name = "datasketches_hll",
        sql = _.sketchSql(SyntheticData.ViewName, SketchFunctions.HllFunctionName),
        relativeSd = None,
        notes = s"Apache DataSketches HLL sketch; lgK=${config.hllLgK}"
      )
    )

  private def timedQuery(spark: SparkSession, sqlText: String): (DataFrame, Long) = {
    val startNs = System.nanoTime()
    val df = spark.sql(sqlText).cache()
    df.count()
    val elapsedMs = (System.nanoTime() - startNs) / 1000000L
    (df, elapsedMs)
  }
}
