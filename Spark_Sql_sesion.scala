


import org.apache.spark.sql.SparkSession

//*    Always scala compiler version should be less than pom.xml
//* pom.xml version should be higher than scala compiler


object Spark_Sql_sesion {
  
  def main(args:Array[String])
  {
    println("Entering in to the main");
    val s = SparkSession.builder().appName("Scala Application").master("local").getOrCreate()
 
   
}}



