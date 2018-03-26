package bigdata.project.system

import org.apache.spark.sql.SparkSession


class RefereeStatsFile(filepath: String) {
  private val spark = SparkSession.builder.master("local[2]").appName("Text Stats").getOrCreate()
}
