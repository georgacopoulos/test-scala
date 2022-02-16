package org.example
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.ArrayType






object ImdbProcess extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Test")
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate();


  def readData(fileName: String): DataFrame = {
    val vFolder="D:/VASGEO/01-Projects/test-scala/src/main/src_data/"
    val df = spark.read
                  .option("delimiter", "\t")
                  .option("header", true)
                  .csv(vFolder+fileName)
    df
  }


  def q1(): DataFrame = {
    val calcAvgNumVotes = readData("title.ratings.tsv").groupBy().agg(avg("numVotes").as("avgNumOfVotes")).cache()
    val df1 = readData("title.ratings.tsv").join(calcAvgNumVotes, lit(1) === lit(1)  , "full").filter("numVotes>=50")
      .withColumn("q1", expr("(numVotes/avgNumOfVotes) * averageRating"))

    val df2 = df1.withColumn("rank",  row_number().over(Window.orderBy(desc("q1")))).filter("rank<=20")
    df2
  }

  def q2(): DataFrame = {
    val numCharacter  = readData("name.basics.tsv")
      .select( col("primaryName"))
      .groupBy("primaryName").agg(sum(lit(1)).as("numConde")).cache()

    val df = readData("name.basics.tsv")
      .select( col("primaryName"),
        explode(split(col("knownForTitles"), ","))).as("knownForTitlesNew").cache()

    val df1 = df.join(broadcast(q1().alias("t2")),
      col("tconst") === col("col"), "inner" ).orderBy(desc("rank")).cache()

    val df2 = df1.alias("t1").join(numCharacter.alias("t2"),
      col("t1.primaryName") === col("t2.primaryName"), "inner" )
      .withColumn("rank",  row_number().over(Window.partitionBy("t1.col").orderBy(desc("t2.numConde"))))
      .select(col("t1.tconst"),col("t1.primaryName"), col("t2.numConde")).filter("rank=1").cache()

    val df3 = readData("title.basics.tsv").alias("t1")
        .join(broadcast(df2.alias("t2")), col("t1.tconst") === col("t2.tconst"), "inner")

    df3.select(col("t2.tconst"),col("t1.primaryTitle"), col("t2.primaryName"), col("t2.numConde"))
  }


q2().show()



}
