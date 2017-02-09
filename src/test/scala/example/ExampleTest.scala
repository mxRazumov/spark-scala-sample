/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
