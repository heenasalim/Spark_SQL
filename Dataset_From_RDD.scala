package DataSets
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object Dataset_From_RDD {
  
  
  def main(args:Array[String])
  {
    
    var config = new SparkConf().setAppName("Creating Configurtion").setMaster("local")
    var sc = new SparkContext(config)
    var sparksql = new SQLContext(sc)
    
   val sparksession  = SparkSession.builder().appName("DataSet Operation").master("local").getOrCreate() 
   import sparksession.implicits._
    //Create RDD
    var variable = sc.parallelize(Seq(1,3,4,5)).toDS();
   //ar file  = sc.parallelize("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt")
    // No need to convert file in dataset use file dataframe above code works fine except for files there are different methods like 
    //craete dataframe
    variable.show();

   
    
    
    
  }
}