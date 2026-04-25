package benchmark.materialization

final case class MaterializationResult(
    dataset: String,
    sketchType: String,
    materializationGroup: String,
    queryName: String,
    inputRows: Long,
    numSketchRows: Long,
    exactCardinality: Double,
    approximateCardinality: Double,
    buildTimeMs: Long,
    exactQueryTimeMs: Long,
    sketchQueryTimeMs: Long,
    sketchTableSizeBytes: Long,
    averageSketchSizeBytes: Double,
    relativeErrorMean: Double,
    relativeErrorMedian: Double,
    relativeErrorP95: Double,
    relativeErrorMax: Double,
    breakEvenQueries: String,
    notes: String
)

object MaterializationResult {
  val CsvHeader: String =
    Seq(
      "dataset",
      "sketch_type",
      "materialization_group",
      "query_name",
      "input_rows",
      "num_sketch_rows",
      "exact_cardinality",
      "approximate_cardinality",
      "build_time_ms",
      "exact_query_time_ms",
      "sketch_query_time_ms",
      "sketch_table_size_bytes",
      "average_sketch_size_bytes",
      "relative_error_mean",
      "relative_error_median",
      "relative_error_p95",
      "relative_error_max",
      "break_even_queries",
      "notes"
    ).mkString(",")

  def toCsv(result: MaterializationResult): String =
    Seq(
      result.dataset,
      result.sketchType,
      result.materializationGroup,
      result.queryName,
      result.inputRows.toString,
      result.numSketchRows.toString,
      formatDouble(result.exactCardinality),
      formatDouble(result.approximateCardinality),
      result.buildTimeMs.toString,
      result.exactQueryTimeMs.toString,
      result.sketchQueryTimeMs.toString,
      result.sketchTableSizeBytes.toString,
      formatDouble(result.averageSketchSizeBytes),
      formatDouble(result.relativeErrorMean),
      formatDouble(result.relativeErrorMedian),
      formatDouble(result.relativeErrorP95),
      formatDouble(result.relativeErrorMax),
      result.breakEvenQueries,
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
