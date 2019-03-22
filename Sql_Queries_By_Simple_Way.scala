package DataFrame_From_Data_Sources
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Sql_Queries_By_Simple_Way {
  
  def main(args:Array[String])
  {
    val sparksession = SparkSession.builder().appName("Dataframe table quries in Simplest way").master("local").getOrCreate();
    import sparksession.implicits._
    val textfile = sparksession.read.option("header","true").option("inferSchema","true").format("csv").load("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt");
    
    textfile.select($"id",$"blood");
    textfile.select(col("id"), col("blood"))
    textfile.select($"id",$"blood").filter($"id" > 5).write.format("csv").save("C:\\Users\\jabin\\Desktop\\output\\filtered_records_for_simple_dataframe.csv");
    //by default saving format is parquet I can also change the csv format
    
    //no need to convert the above dataframe into temp table like below directly we are firing the queries  
    textfile.createOrReplaceTempView("people")
    sparksession.sql("select id,blood from people").show();
   // sparksession.sql("select id,blood from people where id > 5").write.save("C:\\Users\\jabin\\Desktop\\output\\filtered_records_for_table_dataframe")
    
  
    
  
  }
  
}