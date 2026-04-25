package benchmark.materialization

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import benchmark.sketch.PartitionedSketches
import benchmark.{BenchmarkConfig, Metrics, QuerySpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MaterializationBenchmark {
  private val MaterializationGroup: Seq[String] = Seq("date_bucket", "group_id")

  private final case class SketchConfig(
      name: String,
      sketchType: PartitionedSketches.SketchType,
      lgK: Int
  )

  def run(
      spark: SparkSession,
      config: BenchmarkConfig,
      input: DataFrame,
      inputRows: Long,
      exactQueries: Map[String, (DataFrame, Long)],
      queries: Seq[QuerySpec],
      runDirectory: Path
  ): Path = {
    val materializedRoot = runDirectory.resolve("materialized_sketches")
    Files.createDirectories(materializedRoot)

    val sketchConfigs = Seq(
      SketchConfig("theta", PartitionedSketches.Theta, config.thetaLgK),
      SketchConfig("hll", PartitionedSketches.Hll, config.hllLgK)
    )

    val results = sketchConfigs.flatMap { sketchConfig =>
      runForSketch(spark, config, input, inputRows, exactQueries, queries, materializedRoot, sketchConfig)
    }

    val outputPath = runDirectory.resolve("materialization_results.csv")
    val lines = MaterializationResult.CsvHeader +: results.map(MaterializationResult.toCsv)
    Files.write(outputPath, lines.mkString(System.lineSeparator()).getBytes(StandardCharsets.UTF_8))
    outputPath
  }

  private def runForSketch(
      spark: SparkSession,
      config: BenchmarkConfig,
      input: DataFrame,
      inputRows: Long,
      exactQueries: Map[String, (DataFrame, Long)],
      queries: Seq[QuerySpec],
      materializedRoot: Path,
      sketchConfig: SketchConfig
  ): Seq[MaterializationResult] = {
    val sketchDir = materializedRoot.resolve(sketchConfig.name)

    val (sketchTable, buildTimeMs) = timedDataFrame {
      PartitionedSketches.buildSketchTable(
        spark = spark,
        input = input,
        groupColumns = MaterializationGroup,
        distinctColumn = "distinct_key",
        sketchType = sketchConfig.sketchType,
        lgK = sketchConfig.lgK
      )
    }

    sketchTable.write.mode("overwrite").parquet(sketchDir.toString)
    val storedSketchTable = spark.read.parquet(sketchDir.toString).cache()
    val numSketchRows = storedSketchTable.count()
    val sketchTableSizeBytes = directorySizeBytes(sketchDir)
    val averageSketchSizeBytes =
      storedSketchTable.agg(avg(length(col("sketch_bytes")).cast("double")).as("avg_sketch_bytes")).first().getAs[Double]("avg_sketch_bytes")

    queries.map { query =>
      val (exactDf, exactRuntimeMs) = exactQueries(query.name)
      val (estimateDf, sketchQueryTimeMs) = timedDataFrame {
        PartitionedSketches.estimateFromSketchTable(
          spark = spark,
          sketchTable = storedSketchTable,
          targetGroupColumns = query.groupColumns,
          sketchType = sketchConfig.sketchType,
          lgK = sketchConfig.lgK
        )
      }

      val summary = Metrics.summarizeApproximation(spark, query, exactDf, estimateDf)
      val breakEven = breakEvenQueries(buildTimeMs, exactRuntimeMs, sketchQueryTimeMs)

      MaterializationResult(
        dataset = config.dataset,
        sketchType = sketchConfig.name,
        materializationGroup = MaterializationGroup.mkString("+"),
        queryName = query.name,
        inputRows = inputRows,
        numSketchRows = numSketchRows,
        exactCardinality = summary.exactCardinality,
        approximateCardinality = summary.approximateCardinality,
        buildTimeMs = buildTimeMs,
        exactQueryTimeMs = exactRuntimeMs,
        sketchQueryTimeMs = sketchQueryTimeMs,
        sketchTableSizeBytes = sketchTableSizeBytes,
        averageSketchSizeBytes = averageSketchSizeBytes,
        relativeErrorMean = summary.errorMean,
        relativeErrorMedian = summary.errorMedian,
        relativeErrorP95 = summary.errorP95,
        relativeErrorMax = summary.errorMax,
        breakEvenQueries = breakEven,
        notes = "Materialized sketches built once at date_bucket+group_id and reused by merging stored sketch bytes"
      )
    }
  }

  private def timedDataFrame(df: => DataFrame): (DataFrame, Long) = {
    val startNs = System.nanoTime()
    val computed = df.cache()
    computed.count()
    val elapsedMs = (System.nanoTime() - startNs) / 1000000L
    (computed, elapsedMs)
  }

  private def breakEvenQueries(buildTimeMs: Long, exactQueryTimeMs: Long, sketchQueryTimeMs: Long): String = {
    val savedPerQueryMs = exactQueryTimeMs - sketchQueryTimeMs
    if (savedPerQueryMs <= 0) {
      "never"
    } else {
      f"${buildTimeMs.toDouble / savedPerQueryMs.toDouble}%.2f"
    }
  }

  private def directorySizeBytes(path: Path): Long = {
    if (!Files.exists(path)) {
      0L
    } else {
      val stream = Files.walk(path)
      try {
        val iterator = stream.iterator()
        var total = 0L
        while (iterator.hasNext) {
          val current = iterator.next()
          if (Files.isRegularFile(current)) {
            total += Files.size(current)
          }
        }
        total
      } finally {
        stream.close()
      }
    }
  }
}
