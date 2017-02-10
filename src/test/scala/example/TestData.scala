package example

trait TestData extends TestSparkSession {
  implicit def spark = _spark

  val integers = 1 to 2
  val integersArray = integers.toArray
  val integersRDD = spark.sparkContext.parallelize(integers).map(SampleInteger)
  val integersDF = spark.createDataFrame(integersRDD)
}
