package ETL.DataSources

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import ETL.Functions.Get_RDD_SchemaString

class DataSource_HDFS_File (val p_Fichero:String = "", val p_ContieneCabecera:Boolean = false, val p_Separador:String = "", val p_Campos:String = "", val p_Tipos:String = "") extends ETL.Functions.Get_RDD_SchemaString(p_Campos, p_Tipos, p_Separador)  {

  val sc = new SparkContext()
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)


  import sqlContext.implicits._


  private val vGenericRDD = sc.textFile (p_Fichero)

  //NECESITAMOS QUE EN CASO DE QUE CONTIENE CABECERA SEA TRUE SE OMITA LA PRIMERA LINEA Y SI NO NO
  private val vGenericRDD2 = vGenericRDD.mapPartitionsWithIndex{(idx, iter) => if ( (idx == 0) || (p_ContieneCabecera == true) ) iter.drop (1) else iter }

  //Creamos un Schema con la estructura del fichero

  lazy val GenericSchema:StructType
  val rdd = new Get_RDD_SchemaString(p_Campos, p_Tipos, p_Separador)
  val genericSchema = rdd.RDD_Obtener_Esquema()










  val CamposNumerados = new Get_RDD_SchemaString(p_Campos, p_Tipos, p_Separador).ObtenerConjuntoCamposNumerado()

  val vGenericDataFrame = vGenericRDD2.map(_.split("\\" + p_Separador)).map(row => GenericSchema(CamposNumerados)).toDF()

  def ObtenerDataFrame():DataFrame={
    vGenericDataFrame
  }

}
