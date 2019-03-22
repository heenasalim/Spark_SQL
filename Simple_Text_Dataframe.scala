package DataFrame_From_Files

import org.apache.spark.sql.SparkSession

object Simple_Text_Dataframe {
  
  def main(args:Array[String])
  {
    //Create the spark session builder
    val sparksession = SparkSession.builder().appName("DataFrame From Text File").master("local").getOrCreate();
    import sparksession.implicits._
    val text = sparksession.read.option("delimeter","|").option("header", "true")csv("C:\\Users\\jabin\\Desktop\\delimited_files\\pipe_file.txt");
    text.show();
    text.printSchema()
   
    
  }
  
  
}