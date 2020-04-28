package org.apache.spark.cs508

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.sql.SQLContext.implicits


//import org.apache.spark.sql.SQLContext.implicits._
//import sqlContext.implicits._
//import sparkSession.implicits._


object project3 {
  def problem_1(spark: SparkSession): Unit ={
    val df = spark.read
      .csv("./Purchases.csv")
    val newColumns = Seq("TransID","CustID","TransTotal","TransNumItems","TransDesc")
    val temp = df.toDF(newColumns:_*)
    // change column type of the dataframe
    val pdf = temp.withColumn("TransID", temp("TransID").cast(org.apache.spark.sql.types.IntegerType))
      .withColumn("CustID", temp("CustID").cast(org.apache.spark.sql.types.IntegerType))
      .withColumn("TransTotal", temp("TransTotal").cast(org.apache.spark.sql.types.FloatType))
      .withColumn("TransNumItems", temp("TransNumItems").cast(org.apache.spark.sql.types.IntegerType))
//    pdf.show()

    val t1 = pdf.filter("TransTotal <= 600")
//    t1.show()
//
//
    t1.createOrReplaceTempView("t1result")
//    spark.catalog.listColumns("t1result").select("name", "dataType").show()
//    "check the datatype of col"
    val t2 = spark.sql("select TransNumItems, percentile_approx(TransTotal, 0.5) as median, min(TransTotal), max(TransTotal) from t1result group by TransNumItems")
//    t2.show()


    val df1 = spark.read
      .csv("./Customers.csv")
    val newColumns1 = Seq("ID","Name","Age","CountryCode","Salary")
    val temp1 = df1.toDF(newColumns1:_*)
    // change column type of the dataframe
    val cdf = temp1.withColumn("ID", temp1("ID").cast(org.apache.spark.sql.types.IntegerType))
      .withColumn("Age", temp1("Age").cast(org.apache.spark.sql.types.IntegerType))
      .withColumn("CountryCode", temp1("CountryCode").cast(org.apache.spark.sql.types.IntegerType))
      .withColumn("Salary", temp1("Salary").cast(org.apache.spark.sql.types.FloatType))
//    cdf.show()

    val cfilter = cdf.filter(cdf("Age") >= 18 && cdf("Age") <=25)
//    cfilter.show()
    cfilter.createOrReplaceTempView("cfresult")

    val dfjoin = cfilter.join(t1, cfilter("ID") === t1("CustID"), "inner")
//    dfjoin.show()
    dfjoin.createOrReplaceTempView("dfjoinresult")
    val t3 = spark.sql("select ID, Age, sum(TransNumItems) as totalnum, sum(TransTotal) as total from dfjoinresult group by ID, Age")
//    t3.show()

    val t4 =
      t3.as("c1").crossJoin(
        t3.as("c2")
      ).filter("(c1.ID != c2.ID) AND  (c2.ID > c1.ID) " +
        "AND (c2.Age > c1.Age) AND (c1.total > c2.total) AND (c1.totalnum < c2.totalnum)" )
//    t4.show()

    val t5 = t4.select(col("c1.ID").alias("C1 ID"),col("c2.ID").alias("C2 ID"),
      col("c1.Age").alias("Age1"),col("c2.Age").alias("Age2"),
      col("c1.total").alias("TotalAmount1"),col("c2.total").alias("TotalAmount2")
      ,col("c1.totalnum").alias("TotalItemCount1"),col("c2.totalnum").alias("TotalItemCount2"))
    t5.show()

  }

  def problem_2(spark: SparkSession): Unit ={
//    val df = spark.read
//      .csv("./P.csv")
//    df.show(5)


  }
}
