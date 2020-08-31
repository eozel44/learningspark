package workshop5

import org.apache.spark.sql.SparkSession


/** Underlying Engine
 *
 * Phase 1: Analysis
 * The Spark SQL engine begins by generating an abstract syntax tree (AST) for the SQL or DataFrame query.
 * In this initial phase, any columns or table names will be resolved by consulting an internal Catalog,
 * a programmatic interface to Spark SQL that holds a list of names of columns, data types, functions, tables, databases, etc.
 * Once they’ve all been successfully resolved, the query proceeds to the next phase.
 *
 * Phase 2: Logical optimization
 * This phase comprises two internal stages. Applying a standard rule based optimization approach,
 * the Catalyst optimizer will first construct a set of multiple plans and then, using its cost-based optimizer (CBO),
 * assign costs to each plan. These plans are laid out as operator trees; they may include,
 * for example, the process of constant folding, predicate pushdown, projection pruning, Boolean expression simplification, etc.
 * This logical plan is the input into the physical plan.
 *
 * Phase 3: Physical planning
 * In this phase, Spark SQL generates an optimal physical plan for the selected logical plan,
 * using physical operators that match those available in the Spark execution engine.
 *
 * Phase 4: Code generation
 * The final phase of query optimization involves generating efficient Java bytecode to
 * run on each machine. Because Spark SQL can operate on data sets loaded in memory,
 * Spark can use state-of-the-art compiler technology for code generation to speed up
 * execution. In other words, it acts as a compiler. Project Tungsten, which facilitates
 * whole-stage code generation, plays a role here.
 * Just what is whole-stage code generation? It’s a physical query optimization phase that
 * collapses the whole query into a single function, getting rid of virtual function calls
 * and employing CPU registers for intermediate data. The second-generation Tungsten
 * engine, introduced in Spark 2.0, uses this approach to generate compact RDD code
 * for final execution. This streamlined strategy significantly improves CPU efficiency
 * and performance.
 *
 * */


object workshop5 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("workshop5")
      .master("local[2]")
      .getOrCreate()

    //file
    val file = s"/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/departuredelays.csv"


    // Read and create a temporary view
    // Infer schema (note that for larger files you may want to specify the schema)
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(file)
    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    val distances1000 = spark.sql("""SELECT distance,origin,destination FROM us_delay_flights_tbl WHERE distance >1000 ORDER BY distance DESC""")
    distances1000.show(5)

    val delay120 = spark.sql("""SELECT date,delay,origin,destination FROM us_delay_flights_tbl WHERE delay>120 AND origin='SFO' AND destination='ORD' ORDER BY delay DESC""")
    delay120.show(5)


    spark.sql("""SELECT delay, origin, destination,
                         CASE
                         WHEN delay > 360 THEN 'Very Long Delays'
                         WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                         WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                         WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                         WHEN delay = 0 THEN 'No Delays'
                         ELSE 'Early'
                         END AS Flight_Delays
                         FROM us_delay_flights_tbl
                         ORDER BY origin, delay DESC""").show(10)

    // In Scala/Python
    val databases = spark.catalog.listDatabases()
    val tables = spark.catalog.listTables()
    val columns = spark.catalog.listColumns("us_delay_flights_tbl")

    databases.show()
    tables.show()
    columns.show()

  }

}
