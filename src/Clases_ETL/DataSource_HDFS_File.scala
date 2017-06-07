package Clases_ETL

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

class DataSource_HDFS_File {

  private var Ruta:String
  private var Separador:String
  private var SaltoLinea:String
  private var EliminarCabecera:Boolean=false
  private var Nombre_DF:String
  //MÉTODOS
  def Asignar_Ruta(valor:String): Unit ={
    Ruta = valor
  }

  def Asignar_Separador(valor:String): Unit ={
    Ruta = valor
  }

  def Asignar_SaltoLinea(valor:String): Unit ={
    Ruta = valor
  }

  def Asignar_Ruta(valor:String): Unit ={
    Ruta = valor
  }

  def Asignar_Ruta(valor:String): Unit ={
    Ruta = valor
  }

  def Consultar_Ruta():String{
    return Ruta
  }

  def Consultar_Separador():String{
    return Separador
  }

  def Consultar_SaltoLinea():String{
    return SaltoLinea
  }

  def Consultar_Ruta():Boolean{
    return EliminarCabecera
  }

  def Consultar_Ruta():String{
    return Nombre_DF
  }

}

