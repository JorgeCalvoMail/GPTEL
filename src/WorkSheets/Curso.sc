//imports
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import ETL.DataSources.DataSource_HDFS_File


//código
val NuevoDF = new DataSource_HDFS_File( "c:/fich1.txt",
                                    true,
                                    "|",
                                    "ID,Campo1,Campo2",
                                    "integer, string, string").vGenericDataFrame

//mostrar 10 registros
println("LLega")

NuevoDF
NuevoDF.show(10)
