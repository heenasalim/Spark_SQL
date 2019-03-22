package DataFrame_As_SQL_Table

import org.apache.spark.sql._
import org.apache.spark.sql.types._
//import org.apache.spark.sql.types
object Rgister_Dataframe {
  
  def main(args:Array[String])
  {
    
   val sparksession = SparkSession.builder().appName("The Dataframe as SparkSQL").master("local").getOrCreate();
   import sparksession.implicits._;
   //please extend the types for the schema
    val sschema =  StructType(Array(
          StructField("id", IntegerType, true),
          StructField("name",StringType, true),
          StructField("maths_marks",IntegerType,true),
          StructField("science_marks",IntegerType, true),
          StructField("temprature",DoubleType,true)
          )
          );
    val csv_file = sparksession.read.option("header", true).schema(sschema).csv("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt")
    csv_file.createOrReplaceTempView("csv_data_frame_table");
    sparksession.sql("select * from csv_data_frame_table").show();
    
   
  }
  
}