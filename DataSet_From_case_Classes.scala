package DataSets
 import org.apache.spark.SparkConf
 import org.apache.spark.SparkContext
 import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.DatasetHolder._
 import collection.mutable.Seq._

case class examples(id:Int,name:String,temperature:Double)

//CASE CLASSES should be define after import outside the object
object Create_DataSet { 
def main(args:Array[String])
{
 
 
  val sparksession  = SparkSession.builder().appName("DataSet Operation").master("local").getOrCreate() 
  import sparksession.implicits._
  //data set and data frame are providend by only implicits so lets build sparksession
  //sparksession.sparkContext.setLogLevel("WARN")
  
  //1> Create the dataset from the case classes
  val s = Seq(examples(1,"heena",32.0), examples(2,"salim",11.0))
  s.toDS().show(); 
  //Maps , Hashmap and Collections are need to be convert into the sequence in order to create RDD
  //Similarily case classes are needed to convert into the sequence in order to crate the dataset and datframe
  //All higher funtions like collection and case classess should be convert into the sequence
  //to make dataframe or rdd's
 
    
  

 //
 
 
}
}