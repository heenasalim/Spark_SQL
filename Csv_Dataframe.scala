package DataFrame_From_Files
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Csv_Dataframe {
  
 def main(args:Array[String])
 {
   
   val sparksession = SparkSession.builder().appName("The Csv Dataframe").master("local").getOrCreate();
   import sparksession.implicits._
  /* val df_csv = sparksession.read.csv("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt")
   // 1:) Csv File without any schema
   df_csv.show()
   df_csv.select("_c0").show()
   //Here _c0,_c1,_c2,_c3......_cn refers to the columns
   df_csv.select($"_c0", ($"_c2"+ $"_c3")).show()
   df_csv.printSchema();
   
   //now we can provide the schema explicitly
 //1> 
   val new_df_csv = sparksession.read.csv("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt").select(
       '_c0.alias("id").cast(StringType),
       '_c1.alias("name").cast(StringType),
       '_c2.alias("Maths_Marks").cast(IntegerType),
       '_c3.alias("Science_Marks").cast(IntegerType),
       '_c4.alias("Temperature").cast(DoubleType)).where('_c0.isNotNull && '_c1.isNotNull && '_c2 =!= 0 && '_c3 =!= 0 && '_c4 =!= 0)
 
   new_df_csv.select($"id", $"name",$"Temperature").show()
   
 //  2>*/
   val sschema = StructType(
       Array(
       StructField("id", IntegerType,true),
       StructField("name",StringType,true),
       StructField("Maths_Marks_1" ,IntegerType,true),
       StructField("Science_Marks",IntegerType,true),
       StructField("Temperature",DoubleType,true)
       )
       )
       
   var df_external_schema_csv = sparksession.read.option("header", true).schema(sschema).csv("C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt");
   println("Providing the schema externally");   
   df_external_schema_csv.show()
   //If I provide the header option it will remove the existing header and add the new header in that place
   //If we do not provide header option then it will consider first header line as record
   //So generally use this explicit schema only for non schema file else useinferscema
   
  
   
   // 2:) Schema using header
   //read the file by different way
    //Key :-
    //header :- It considers first line as columns line
    //Inferschema :-convert every string columns into the proper datatypes
    //describe:-use to show the highest values,lowest values and mean values in the files
    val df_csv_header = sparksession.read.option("header", "true").option("inferSchema","true").csv(
       "C:\\Users\\jabin\\Desktop\\delimited_files\\csv.txt");
    df_csv_header.printSchema();
    df_csv_header.describe().show()
    df_csv_header.show()
    df_csv_header.columns;
    df_csv_header.select($"id").show();
    df_csv_header.select($"name", $"maths_marks").show();
    //filtering the columns values in scala
    //=== is use for matching condition in sparksql
    df_csv_header.filter($"name" === "heena").show();
    df_csv_header.filter($"maths_marks" > 4000).show();
    df_csv_header.filter($"name"==="heena"  && $"id" ===1 && $"maths_marks" > 2000).show()
    //Find the toppers in the maths 
    println("Print th information of the toppper in the class in maths ")
    var toppers = df_csv_header.select($"name", $"id",$"maths_marks",$"temperature").filter($"maths_marks" >= 5000)
    toppers.count()
    toppers.show()
    //filtering using sql attotaions
    df_csv_header.filter("name = 'heena'");
   // df_csv_header.filter("maths_marks > 2000").show();
   // df_csv_header.filter("name = 'heena' AND  id = 1 AND maths_marks > 3000").show()
   df_csv_header.select(avg($"maths_marks")).show();
   //adding te new columns
  df_csv_header.withColumn("New_Tempterature", $"temperature"+$"id").show();
  df_csv_header.select(avg($"temperature")).show();
  
   //group operations
  //if i want to do group by with some mathfunctions always use agg with groupby
  df_csv_header.groupBy($"id").count();
  df_csv_header.groupBy($"id").agg(sum($"maths_marks")).show()
   
          
   //  I have not given any schema to the file therefore it is considering columns as _c0,_c1,_c2
  // 3:) Schema using non-header
   
   
 }
  
}

