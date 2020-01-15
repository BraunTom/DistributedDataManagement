package de.hpi.ind_discovery_RobertDropTableStudents

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val tpchFlattenedTables = inputs.map(inputName => {
      // Load the CSV file corresponding to each TPCH table into an RDD
      val table = spark
        .read
        .option("inferSchema", "false")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(inputName)
        .toDF()

      // "Flatten" the table so each cell is converted to a row along with its corresponding column name
      // E.g. Before:
      // |--------------|
      // | COLA  | COLB |
      // |--------------|
      // | 1     | 2    |
      // | 3     | 4    |
      // |--------------|
      // After:
      // |--------------|
      // | VALUE | COL  |
      // |--------------|
      // | 1     | COLA |
      // | 2     | COLB |
      // | 3     | COLA |
      // | 4     | COLB |
      // |--------------|
      val columns = table.columns // TODO: Should broadcast?
      table.flatMap(r => (0 until r.length).map(i => r.getString(i)).zip(columns))
    })

    // Concatenate all "flattened" tables of TPCH together
    val tpchTuples = tpchFlattenedTables.reduce((a,b) => a.union(b))

    // Calculate attribute sets: For each value, create a set containing the names of all the
    // tables it appears in (after this step, the values are not longer important and are dropped)
    // E.g. Before:
    // |--------------|
    // | VALUE | COL  |
    // |--------------|
    // | 1     | COLA |
    // | 1     | COLA |
    // | 1     | COLB |
    // | 1     | COLC |
    // | 2     | COLA |
    // | 2     | COLC |
    // | 3     | COLC |
    // | 5     | COLC |
    // |--------------|
    // After:
    // |----------------|
    // | COLS           |
    // |----------------|
    // | COLA,COLB,COLC |
    // | COLA,COLC      |
    // | COLC           |
    // | COLC           |
    // |----------------|
    val attributeSets = tpchTuples.groupByKey { case (value, _) => value }
      .mapGroups { case (_, iterator) => iterator.map(x => x._2).toSet }

    // Calculate inclusion lists: For each attribute set,
    // create a tuple (value, attribute set - value)
    // E.g. Before:
    // |----------------|
    // | COLS           |
    // |----------------|
    // | COLA,COLB,COLC |
    // | COLA,COLC      |
    // | COLC           |
    // | COLC           |
    // |----------------|
    // After:
    // |-------------------|
    // | VALUE | COLS      |
    // |-------------------|
    // | COLA  | COLB,COLC |
    // | COLB  | COLA,COLC |
    // | COLC  | COLB,COLC |
    // | COLA  | COLC      |
    // | COLC  | COLA      |
    // | COLC  | (Empty)   |
    // | COLC  | (Empty)   |
    // |-------------------|
    val inclusionLists = attributeSets.flatMap(r => r.map(v => (v, r - v)))

    // Calculate INDs: Group all inclusion lists by key, and intersect
    // their corresponding column sets all together. Those are the INDs
    // E.g. Before:
    // |-------------------|
    // | VALUE | COLS      |
    // |-------------------|
    // | COLA  | COLB,COLC |
    // | COLB  | COLA,COLC |
    // | COLC  | COLB,COLC |
    // | COLA  | COLC      |
    // | COLC  | COLA      |
    // | COLC  | (Empty)   |
    // | COLC  | (Empty)   |
    // |-------------------|
    // After:
    // |-------------------|
    // | VALUE | COLS      |
    // |-------------------|
    // | COLA  | COLC      |
    // | COLB  | COLA,COLC |
    // |-------------------|
    val inds = inclusionLists.groupByKey { case(k, _) => k }
        .mapGroups { case (k, sets) => (k, sets.map(a => a._2).reduce((a, b) => a.intersect(b))) }
        .filter(r => r._2.nonEmpty)

    // Collect and print the INDs (in order)
    inds.collect().sortBy(x => x._1).foreach(x => println(x._1 + " < " + x._2.mkString(", ")))
  }
}
