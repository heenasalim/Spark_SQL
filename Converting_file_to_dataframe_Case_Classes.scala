package Case_Classes

import org.apache.spark.sql.SparkSession
case class student_portal (id:Int,Roll_No:Int,Marks:Int)

object Case_Classes_operations_Portal {
 
  def main(args:Array[String])
  {
    
    val sparksession = SparkSession.builder().appName("Case Operations").master("local").getOrCreate()
    import sparksession.implicits._
    val textfiledataframe = sparksession.sparkContext.textFile("C:\\Users\\jabin\\Desktop\\delimited_files\\student_portal.txt").map( x=>x.split(",")).map(s =>
    student_portal( s(0).toInt,s(1).toInt,s(2).toInt)).toDF()
    //This is reading text  file and sperperating by comas then each first record second and third record are passto student ojet
    
   //output student_portal(1,11,2000)
   //student_portal(2,390,1233)
   //student_portal(4,211,5211)
   //sc.textfile("C:\\Users\\jabin\\Desktop\\delimited_files\\student_portal.txt") is RDD
   //sc.parllelize("C:\\Users\\jabin\\Desktop\\delimited_files\\student_portal.txt")
   //sc.makeRDD("C:\\Users\\jabin\\Desktop\\delimited_files\\student_portal.txt")
    textfiledataframe.createOrReplaceTempView("student_info");
    sparksession.sql("select * from student_info where Roll_no > 5").show();
    
    // we can also access the element base on he feild index eg
    sparksession.sql("select * from student_info").map( s => s(0) + "Marks_got" ).show();
    sparksession.sql("select * from student_infp").map(s => s.getAs[String]("Marks")).show()
    
    ;
    
    
    
  
  }
  }
