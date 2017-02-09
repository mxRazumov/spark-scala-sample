package example

class ExampleTest extends TestSparkSession {

  import example.Example._

  implicit def spark = _spark

  test("Numbers generate an array of Int") {
    val n = 3
    val numbers = new NumbersGenerator(n)
    val arr = numbers.generate()

    assert(arr === Array(0, 1, 2))
  }

  test("Create DataFrame") {
    val n = 3
    val df = createDataFrame(n)

    assert(df.collect().map(r => r.getAs[Int]("n")) === Array(0, 1, 2))
  }

  test("Calculate") {
    val n = 3
    val df = createDataFrame(n) // (0, 1, 2)
    val sum = calcNumbers(df)

    assert(sum.collect().map(r => r.getAs[Int]("diff")) === Array(0, 1, 2))
  }
}
