
package ETL.DataSources

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import ETL.Functions.Generic_RDD_SchemaString

class DataSource_HDFS_File (val p_Fichero:String = "", val p_ContieneCabecera:Boolean = false, val p_Separador:String = "", val p_Campos:String = "", val p_Tipos:String = "") extends ETL.Functions.Generic_RDD_SchemaString()  {

  val sc = new SparkContext()
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  private val vGenericRDD = sc.textFile (p_Fichero)
  //NECESITAMOS QUE EN CASO DE QUE CONTIENE CABECERA SEA TRUE SE OMITA LA PRIMERA LINEA Y SI NO NO
  private val vGenericRDD2 = vGenericRDD.mapPartitionsWithIndex{(idx, iter) => if ( (idx == 0) || (p_ContieneCabecera == true) ) iter.drop (1) else iter }


  //tenemos que pasarlo de RDD tipo STRING a RDD tipo ROW
  val vGenericRDD3 = vGenericRDD2.map( line => line.split(p_Separador(0)))
  val cGenericRDD4 = vGenericRDD3.map(a => Row.fromSeq(a))


  var vGenericSchema = new Generic_RDD_SchemaString().RDD_Obtener_Esquema(p_Campos, p_Tipos, p_Separador)
  val vGenericDataFrame = sqlContext.createDataFrame(cGenericRDD4,vGenericSchema)

}
/*
object DataSource_HDFS_File{

  def apply(
            p_Fichero: String = "",
            p_ContieneCabecera: Boolean = false,
            p_Separador: String = "",
            p_Campos: String = "",
            p_Tipos: String = ""
            ): DataSource_HDFS_File = ETL.DataSources.DataSource_HDFS_File(
            p_Fichero,
            p_ContieneCabecera,
            p_Separador,
            p_Campos,
            p_Tipos
  )

}*/

