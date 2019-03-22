package DataSets

import org.apache.spark.sql._
object toDs {
  def main(args:Array[String])
{
 
 
  //data set and data frame are providend by only implicits so lets build sparksession
  val sparksession  = SparkSession.builder().appName("DataSet Operation").master("local").getOrCreate() 
  import sparksession.implicits._

 
  //Create the dataset from the Sequence
  val s1 = Seq(1,3,4,5).toDS()
  s1.map(x => x + 1).show()
  //Always use show in sparksql for printing 
  //The map function is applicable to both Scala's Mutable and Immutable collection data structures.
}}