package benchmark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SyntheticData {
  val ViewName: String = "benchmark_input"

  def build(spark: SparkSession, config: BenchmarkConfig): DataFrame = {
    val safeGroups = math.max(config.groups, 1)
    val safeDistinctValues = math.max(config.distinctValues, 1L)
    val safePartitions = math.max(config.partitions, 1)
    val safeDays = math.max(config.days, 1)

    spark
      .range(0L, config.rows, 1L, safePartitions)
      .select(
        col("id"),
        concat(lit("key_"), pmod(col("id"), lit(safeDistinctValues)).cast("string")).as("distinct_key"),
        concat(lit("group_"), pmod(col("id"), lit(safeGroups)).cast("string")).as("group_id"),
        date_add(to_date(lit("2024-01-01")), pmod(col("id"), lit(safeDays)).cast("int")).as("date_bucket")
      )
  }
}
