package enesuguroglu

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, expr, from_json}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}

object OmsDeliveryDemandsTest {

  def main(args: Array[String]): Unit = {

    //Initiate spark session
    val spark = SparkSession.builder()
      .appName("oms_returned_test")
      .master("local[3]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    spark.sparkContext.setLogLevel("ERROR")

    //Read data from kafka
    val dataStreamReader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "alizew04.infoshop.com.tr:6667,alizew05.infoshop.com.tr:6667,alizew06.infoshop.com.tr:6667")
      .option("subscribe", "oms_delivery_bulk_topic")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val schema = StructType(List(
      StructField("id", StringType),
      StructField("code", StringType),
      StructField("insertedDate", StringType),
      StructField("deliveryOrderLines",ArrayType(StructType(List(
        StructField("orderLineId", StringType),
        StructField("orderNumber", StringType),
        StructField("order_date", StringType)
      )))),
      StructField("demands", ArrayType(StructType(List(
      StructField("type", StringType),
      StructField("date", StringType),
      StructField("parameters", MapType(StringType,StringType)
      ))))),
    StructField("receivedAt", StringType)
      ))

    //Deserialization
    val value_df = dataStreamReader.select(from_json(col("value").cast("string"),schema).alias("value"))

    value_df.printSchema()

    //Flattening-1
    val explode_df = value_df.selectExpr("value.id", "value.code", "value.deliveryOrderLines as deliveryorderlines",
      "value.demands as demands", "value.receivedAt as receivedat", "value.insertedDate as inserteddate"
    )

    explode_df.printSchema()

    //Exploding
    val explode_df2 = explode_df
      .withColumn("deliveryorderlinesorderlineexp", explode(col("deliveryorderlines")))
      .withColumn("demandsexp", explode(col("demands")))
      .drop("deliveryorderlines")
      .drop("demands")

    explode_df2.printSchema()

    //Flattening-2
    val explode_df3 = explode_df2
      .withColumn("deliveryorderlinesorderlineid", expr("deliveryorderlinesorderlineexp.orderLineId"))
      .withColumn("deliveryorderlinesordernumber", expr("deliveryorderlinesorderlineexp.orderNumber"))
      .withColumn("order_date", expr("deliveryorderlinesorderlineexp.order_date"))
      .withColumn("demandsdate", expr("demandsexp.date"))
      .withColumn("demandsparameters", expr("demandsexp.parameters"))
      .withColumn("demandstype", expr("demandsexp.type"))
      .drop("deliveryorderlinesorderlineexp")
      .drop("demandsexp")

    explode_df3.printSchema()

    //deliverycreatedate column definition
    val df = explode_df3
      .withColumn("deliverycreateddate", expr("""from_unixtime(unix_timestamp(from_utc_timestamp(to_timestamp(unix_timestamp(insertedDate , "yyyy-MM-dd'T'HH:mm")), 'Europe/Istanbul')),'yyyy-MM-dd') """))
      .drop("inserteddate")

    //Write streaming data using foreachBatch function
    val query = df.writeStream
      .queryName("OmsDeliveryReturned")
      .foreachBatch(writeBatchData _)
      .option("checkpointLocation", "C:\\Users\\eugurluoglu\\Documents\\Spark\\Spark_Scala\\OmsDeliveryDemandsTest\\chk").start()
    query.awaitTermination()

  }

  //Define function to write stream data for each batch
  def writeBatchData(batchDF: DataFrame, epoch: Long): Unit = {
    batchDF
      .write.format("console")
      .save()
  }

}
