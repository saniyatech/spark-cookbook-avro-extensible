/*
#
# MIT License
#
# Copyright (c) 2017 Saniya Tech Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
# associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
# LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#
*/
/* SparkApp.scala */
package org.saniyatech.spark

import java.nio.file.Files
import java.sql.Date

import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Account (
                     name: String,
                     manager: Person,
                     addresses: Array[Address],
                     ratio: Float,
                     transactions: Array[String]
                   )

case class AccountV2 (
                     name: String,
                     manager: PersonV2,
                     addresses: Array[AddressV2],
                     ratio: Float,
                     transactions: Map[String, String]
                   )

case class Person (
                    name: String,
                    dob: String
                  )

case class PersonV2 (
                    name: String,
                    dateOfBirth: Date
                  )

case class Address (
                     addressLine: String,
                     city: String,
                     state: String,
                     zipCode: Int
                   )

case class AddressV2 (
                     addressLine: String,
                     city: String,
                     state: String,
                     zipStr: String
                   )

object SparkApp {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Spark App")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val path = Files.createTempDirectory("schema-test")

    import spark.implicits._

    // Generate data to be serialized
    val data = Account(
      "Simple Account v1",
      Person(
        "Johnny Doe",
        "2000-01-02"
      ),
      Array(
        Address("123 Main St", "Anytown", "ZZ", 12345)
      ),
      0.983f,
      Array("A transaction", "no other transactions")
    )
    val dataset = spark.createDataset(Seq(data)).as[Account]

    // Print schema and data for the Account dataset
    dataset.printSchema()
    dataset.show()

    // Serialize Account dataset to Avro format
    dataset.write
      .mode(SaveMode.Overwrite)
      .avro(path.toString)

    // Simulate a time in future where dataset is being reloaded after the schema has changed
    // Deserialize or load Account data as AccountV2

    val convertAddressToV2 = udf((addresses: Seq[Row]) =>
      addresses.map(row =>
        AddressV2(row.getAs("addressLine"), row.getAs("city"), row.getAs("state"), row.getAs("zipCode").toString)
      )
    )

    val convertPersonToV2 = udf((row: Row) =>
      PersonV2(row.getAs("name"), Date.valueOf(row.getAs[String]("dob")))
    )

    val convertTransactions = udf((transactions: Seq[String]) =>
      transactions.zipWithIndex.map(t => t._2.toString -> t._1).toMap
    )

    val loadedDataset = spark.read
      .avro(path.toString)
      .withColumn("manager", convertPersonToV2(col("manager")))
      .withColumn("addresses", convertAddressToV2(col("addresses")))
      .withColumn("transactions", convertTransactions(col("transactions")))
      .as[AccountV2]

    // Print legacy Account data as AccountV2
    loadedDataset.printSchema()
    loadedDataset.show()

    spark.stop()

  }
}
