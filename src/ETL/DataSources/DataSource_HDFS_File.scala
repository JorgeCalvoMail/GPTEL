/* ---------------------------------------------------------------------------------------------------------------------
 *  Paquete.Clase : " Clases_ETL.DataSource_HDFS_File "
 *----------------------------------------------------------------------------------------------------------------------
 * Atributos Objeto:
 * ---------------------------------------------------------------------------------------------------------------------
 * Ruta => Ruta del fichero HDFS
 * Separador => Separador del fichero
 * SaltoLinea => Caracter de Salto de Línea
 * EliminarCabecera => TRUE | FALSE
 * Nombre_DF => Nombre del DataFrame que carga
 * ---------------------------------------------------------------------------------------------------------------------
 * Asignar_Ruta => Permite asignar la ruta del objeto
 * Asignar_Separador => Permite asignarle el separador de campos
 * Eliminar_Cabecera => Permite indicar si deseamos eliminar la cabecera del fichero
 * Asignar_SaltoLinea => Permite Indicar el Salto de Linea que Contiene el Fichero
 *
 * Consultar_Ruta => Permite Consultar el atributo Separador
 * Consultar_Separador => Permite Consultar el atributo SaltoLinea
 * Consultar_SaltoLinea => Permite Consultar el atributo EliminarCabecera
 * Consultar_Nombre_DF => Permite Consultar el atributo Nombre_DF
 * -------------------------------------------------------------------------------------------------------------------*/
package ETL.DataSources
class DataSource_HDFS_File {

  private var Ruta: String = ""
  private var Separador: String = ""
  private var SaltoLinea: String = ""
  private var EliminarCabecera: Boolean = false
  private var Nombre_DF: String = ""

  //MÉTODOS
  def Asignar_Ruta(valor:String){
    Ruta = valor
  }

  def Asignar_Separador(valor:String){
    Separador = valor
  }

  def Asignar_SaltoLinea(valor:String){
    SaltoLinea = valor
  }

  def Asignar_Eliminar_Cabecera(valor:Boolean){
    EliminarCabecera = valor
  }

  def Asignar_Nombre_DF(valor:String){
    Nombre_DF = valor
  }

  def Consultar_Ruta():String = {
    Ruta
  }

  def Consultar_Separador():String = {
    Separador
  }

  def Consultar_SaltoLinea():String = {
    SaltoLinea
  }

  def Consultar_EliminarCabecera():Boolean = {
    EliminarCabecera
  }

  def Consultar_Nombre_DF():String = {
    Nombre_DF
  }

}

