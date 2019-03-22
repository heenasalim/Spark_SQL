package DataSets

 import org.apache.spark.sql._
object Dataset_From_Create_Dataset {
  
def main(args:Array[String])
{

//data set and data frame are providend by only implicits so lets build sparksession
  val sparksession  = SparkSession.builder().appName("DataSet Operation").master("local").getOrCreate() 
  import sparksession.implicits._

 //Crating Dataset from raw file directly
 val sc = sparksession.sparkContext
 val csv_file_1 = sc.textFile("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt");
 val ds =sparksession.createDataset(csv_file_1);

 
 // Creating dataset from the rdd sequence is not allowed in createdataframe
 
 //sparksession.createDataFrame(sc.parallelize(Seq(1,4,4,5)));
 
//Create dataset from te squence directly
 
 //sparksession.createDataFrame(Seq(1,4,4,5));
 

}
}