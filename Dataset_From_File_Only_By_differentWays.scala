package DataSets


 import org.apache.spark.sql._
 import org.apache.spark.SparkConf
 import org.apache.spark.SparkContext
object Dataset_From_File {
  
//data set and data frame are providend by only implicits so lets build sparksession
  val sparksession  = SparkSession.builder().appName("DataSet Operation").master("local").getOrCreate() 
  import sparksession.implicits._
//sparksession.sparkContext.setLogLevel("WARN")
  
// 1> File dataset from dataframe using alias
 val csv_file= sparksession.read.csv("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt").as("Student")
 //Its Dataframe
 
//2> File dataset from raw file using createdataset
 var config = new SparkConf().setAppName("Creating Configurtion").setMaster("local")
 var sc = new SparkContext(config)
 val file1 = sc.textFile("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt");
 import sparksession.implicits._
 val ds =sparksession.createDataset(file1);
 ds.show()

//3> File Dataset from raw file using toDS function
 val file2 = sc.textFile("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt").toDS();
 file2.show()
 
 
}