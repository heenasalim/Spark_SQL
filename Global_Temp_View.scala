package DataFrame_As_SQL_Table
import org.apache.spark.sql._

object Global_Temp_View {
  
 def main(args:Array[String])
  {
 // Global Temporary View

//Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates. If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates, you can create a global temporary view. Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.

  val sparksession = SparkSession.builder().appName("GloabalTempView").master("local")
  .getOrCreate();
  
  import sparksession.implicits._
  var csv_df= sparksession.read.option("header","true").option("schema","inferSchema").csv("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt")
  csv_df.show()
  csv_df.createGlobalTempView("student");
  //for reference i can use global_temp not at a creation time ,it will craete the table in glovbal database 
  //only by default
  sparksession.sql("select * from global_temp.student").show()
}
  
 
 
}