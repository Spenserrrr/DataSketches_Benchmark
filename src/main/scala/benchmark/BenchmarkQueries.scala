package benchmark

final case class QuerySpec(
    name: String,
    groupColumns: Seq[String],
    distinctColumn: String = "distinct_key"
) {
  def exactSql(tableName: String): String =
    distinctSql(tableName, s"COUNT(DISTINCT $distinctColumn)")

  def approxSql(tableName: String, relativeSd: Double): String =
    distinctSql(tableName, s"approx_count_distinct($distinctColumn, $relativeSd)")

  private def distinctSql(tableName: String, aggregate: String): String = {
    if (groupColumns.isEmpty) {
      s"SELECT $aggregate AS distinct_count FROM $tableName"
    } else {
      val groups = groupColumns.mkString(", ")
      s"SELECT $groups, $aggregate AS distinct_count FROM $tableName GROUP BY $groups"
    }
  }
}

object BenchmarkQueries {
  val BaselineQueries: Seq[QuerySpec] = Seq(
    QuerySpec(name = "global_distinct", groupColumns = Seq.empty),
    QuerySpec(name = "grouped_distinct", groupColumns = Seq("group_id"))
  )
}
