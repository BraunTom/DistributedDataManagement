package de.hpi.ind_discovery_RobertDropTableStudents

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main extends App {

  override def main(args: Array[String]): Unit = {
    var path = "TPCH/"
    var cores = 4

    for (
      i <- args.indices
      if i % 2 == 0
    ) {
      args(i) match {
        case "--cores" => cores = args(i+1).toInt
        case "--path" => path = args(i+1)
      }
    }

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("INDDiscoveryRobertDropTableStudents")
      .master(s"local[$cores]") // local, with as many worker cores as specified
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions
    // See https://stackoverflow.com/a/45704560
    spark.conf.set("spark.sql.shuffle.partitions", (2*cores).toString)

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputPathFiles = new File(path).listFiles
    if (inputPathFiles == null) {
      println("Data directory not found")
      sys.exit(1)
    }
    val inputCsvFileNames = inputPathFiles.map(_.getPath).filter(_.endsWith(".csv")).toList

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    time {Sindy.discoverINDs(inputCsvFileNames, spark)}
  }
}
