package DataSets

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


case class student(id:Int,name:String,blood:Long,Mobile_No:Long,weight:Double)

//alias only works with the case classes

object Dataset_From_Alias_Method {
def main(args:Array[String]){
 val config = new SparkConf().setAppName("Setting configuration for appliction").setMaster("local")
 val sc = new SparkContext(config)
 val sparksession = SparkSession.builder().appName("Sequence").master("local").getOrCreate()
 import sparksession.implicits._
 //sEQUENCE cannot be use as follows
 //Seq(1,"Rahul",'A').toDF().as[employee].show
 //Seq(3,4,5,6).toDS()

// Case Class
 Seq(student(2,"salim",3000,3000,45.00)).toDF().as[student].show();
 //For File
import sparksession.implicits._
val file = sparksession.read.csv("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt").as[student].show()
 
} 
}