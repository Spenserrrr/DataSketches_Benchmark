package benchmark

final case class BenchmarkResult(
    dataset: String,
    queryName: String,
    method: String,
    inputRows: Long,
    numGroups: Long,
    exactCardinality: Double,
    approximateCardinality: Double,
    runtimeMs: Long,
    relativeErrorMean: Double,
    relativeErrorMedian: Double,
    relativeErrorP95: Double,
    relativeErrorMax: Double,
    relativeSd: Option[Double],
    notes: String
)

object BenchmarkResult {
  val CsvHeader: String =
    Seq(
      "dataset",
      "query_name",
      "method",
      "input_rows",
      "num_groups",
      "exact_cardinality",
      "approximate_cardinality",
      "runtime_ms",
      "relative_error_mean",
      "relative_error_median",
      "relative_error_p95",
      "relative_error_max",
      "relative_sd",
      "notes"
    ).mkString(",")

  def toCsv(result: BenchmarkResult): String =
    Seq(
      result.dataset,
      result.queryName,
      result.method,
      result.inputRows.toString,
      result.numGroups.toString,
      formatDouble(result.exactCardinality),
      formatDouble(result.approximateCardinality),
      result.runtimeMs.toString,
      formatDouble(result.relativeErrorMean),
      formatDouble(result.relativeErrorMedian),
      formatDouble(result.relativeErrorP95),
      formatDouble(result.relativeErrorMax),
      result.relativeSd.map(formatDouble).getOrElse(""),
      result.notes
    ).map(escapeCsv).mkString(",")

  private def formatDouble(value: Double): String =
    f"$value%.8f"

  private def escapeCsv(value: String): String = {
    val needsQuoting = value.exists(ch => ch == ',' || ch == '"' || ch == '\n' || ch == '\r')
    if (!needsQuoting) value
    else "\"" + value.replace("\"", "\"\"") + "\""
  }
}
