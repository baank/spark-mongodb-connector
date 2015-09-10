package nsmc.sql

import java.util.Date

import com.mongodb.casbah.Imports._
import nsmc.TestConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

class UserSchemaTests extends FlatSpec with Matchers {
  "collection with matching, flat user-specified schema" should "query correctly with *" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {

      sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE dataTable (_id string, key int, s string)
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT * FROM dataTable")

      val fields = data.schema.fields
      fields should have size (3)
      fields(0) should be (new StructField("_id", StringType, true))
      fields(1) should be (new StructField("key", IntegerType, true))
      fields(2) should be (new StructField("s", StringType, true))

      data.count() should be(300000)
      val firstRec = data.first()

      // don't match the id
      firstRec.getInt(1) should be (1)
      firstRec.getString(2) should be ("V1")

    } finally {
      sc.stop()
    }
  }

  "collection with matching, disordered flat user-specified schema" should "query correctly with *" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {

      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE dataTable (key INT, s STRING, _id STRING)
           |USING nsmc.sql.MongoRelationProvider
           |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT * FROM dataTable")

      val fields = data.schema.fields
      fields should have size (3)
      fields(0) should be (new StructField("key", IntegerType, true))
      fields(1) should be (new StructField("s", StringType, true))
      fields(2) should be (new StructField("_id", StringType, true))

      data.count() should be(300000)
      val firstRec = data.first()

      // don't match the id
      firstRec.getInt(0) should be (1)
      firstRec.getString(1) should be ("V1")

    } finally {
      sc.stop()
    }
  }

  "collection with incomplete user-specified schema" should "return specified columns when queried with *" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {

      sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE dataTable (_id string, s string)
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT * FROM dataTable")

      val fields = data.schema.fields
      fields should have size (2)
      fields(0) should be (new StructField("_id", StringType, true))
      fields(1) should be (new StructField("s", StringType, true))

      data.count() should be(300000)
      val firstRec = data.first()

      // don't match the id
      firstRec.getString(1) should be ("V1")

    } finally {
      sc.stop()
    }
  }

  "collection with various atomic datatypes" should "query correctly with a user provided schema" in {
    val mongoClient = MongoClient(TestConfig.mongodHost, TestConfig.mongodPort.toInt)
    val db = mongoClient.getDB("test")

    try {
      val col = db(TestConfig.scratchCollection)
      col.drop()

      col += MongoDBObject("f1" -> 1) ++
        ("f2" -> 3.14) ++
        ("f3" -> "hello") ++
        ("f9" -> new Date(0))
    } finally {
      mongoClient.close()
    }

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {

      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE dataTable (
           |  f1 INT,
           |  f2 DOUBLE,
           |  f3 STRING,
           |  f9 DATE
           |)
           |USING nsmc.sql.MongoRelationProvider
           |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.scratchCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT * FROM dataTable")

      val fields = data.schema.fields

      fields should have size (4)
      fields(0) should be (new StructField("f1", IntegerType, true))
      fields(1) should be (new StructField("f2", DoubleType, true))
      fields(2) should be (new StructField("f3", StringType, true))
      fields(3) should be (new StructField("f9", DateType, true))

      val recs = data.collect()
      recs should have size (1)
      val first = recs(0)
      first should have size (4)
      first(0) should be (1)
      first(1) should be (3.14)
      first(2) should be ("hello")
      first(3) should be (new Date(0))

    } finally {
      sc.stop()
    }
  }
}
