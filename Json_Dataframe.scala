package DataFrame_From_Files
import org.apache.spark.sql.SparkSession
import org.apache.spark._

object Json_Dataframe {
   
  def main(args:Array[String]) = {
  println("Inside the main")
  val sparksession = SparkSession.builder().appName("SparkSQL").master("local")getOrCreate()
  //I need to extend the created saprksession object inorder to use sparksession functionalities
  import sparksession.implicits._;
  val jfile =  sparksession.read.json("C:\\Users\\jabin\\Desktop\\delimited_files\\json.txt")
  println("Print the contents.....");
  jfile.show();
  println("Schema is already defined in the files");
  println("Printing the file schema ...")
  jfile.printSchema()
  //Printing the first column
  jfile.select("Student.id").show()
  //printing two columns out of three
  jfile.select($"Student.id",$"Student.Marks").show()
  //Increasing balance of the Dmart by 5
  jfile.select($"Student.id",$"Student.Marks" + 5).show()
  //filtering the data base on the student id
  jfile.filter($"Student.Marks" < 60).show()
  
}}