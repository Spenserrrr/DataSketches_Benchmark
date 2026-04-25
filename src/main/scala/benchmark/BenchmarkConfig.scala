package benchmark

final case class BenchmarkConfig(
    dataset: String = "synthetic",
    inputPath: String = "",
    maxRows: Long = 0L,
    rows: Long = 1000000L,
    distinctValues: Long = 100000L,
    groups: Int = 100,
    days: Int = 30,
    partitions: Int = 4,
    relativeSd: Double = 0.05,
    thetaLgK: Int = 12,
    hllLgK: Int = 12,
    outputRoot: String = "results"
)

object BenchmarkConfig {
  def parse(args: Array[String]): BenchmarkConfig = {
    val values = args.toList
      .sliding(2, 2)
      .collect { case flag :: value :: Nil if flag.startsWith("--") =>
        flag.drop(2) -> value
      }
      .toMap

    BenchmarkConfig(
      dataset = values.getOrElse("dataset", "synthetic"),
      inputPath = values.getOrElse("input", ""),
      maxRows = values.get("max-rows").map(_.toLong).getOrElse(0L),
      rows = values.get("rows").map(_.toLong).getOrElse(1000000L),
      distinctValues = values.get("distinct").map(_.toLong).getOrElse(100000L),
      groups = values.get("groups").map(_.toInt).getOrElse(100),
      days = values.get("days").map(_.toInt).getOrElse(30),
      partitions = values.get("partitions").map(_.toInt).getOrElse(4),
      relativeSd = values.get("relative-sd").map(_.toDouble).getOrElse(0.05),
      thetaLgK = values.get("theta-lg-k").map(_.toInt).getOrElse(12),
      hllLgK = values.get("hll-lg-k").map(_.toInt).getOrElse(12),
      outputRoot = values.getOrElse("output-root", values.getOrElse("output", "results"))
    )
  }
}
