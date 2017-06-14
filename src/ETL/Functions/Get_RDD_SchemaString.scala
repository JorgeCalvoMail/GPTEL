
package ETL.Functions

/*_________________________________________________________________________________________________
* ETL.Functions.Get_RDD_SchemaString
* |
* | Este objeto es realmente una función la cual tiene como función el componer un objeto
* | válido para posteriormente emplear como esquema para componer un DataFrame.
* |
* | RECIBE NECESARIAMENTE LOS SIGUIENTES PARAMETROS:
* |
* |     1) Cadena de texto compuesta de nombres de campos separados por un separador
* |     2) Cadena de texto compuesta de los tipos de los campos pasados en el anterior parametro
* |     3) Separador de campos empleado en ambas cadenas
* ________________________________________________________________________________________________*/ //Desplegar/Replegar esta DESCRIPCION para ver o no el detalle de la funcionalidad cubierta
import scala.collection.Map
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._;

class Get_RDD_SchemaString (val p_CadenaCampos:String = "", val p_CadenaTipos:String = "", p_Separador:String = "") {

  val vCampos:String = p_CadenaCampos
  val vTipos:String = p_CadenaTipos
  var vSeparador:String = p_Separador
  var vValidacionCampos:Boolean = true


  //Validacion Básica dentro de su constructor principal
  if ( vSeparador.length != 1 ){ vValidacionCampos = false }
  if ( vCampos.isEmpty | vTipos.isEmpty | vSeparador.isEmpty ){ vValidacionCampos = false }


  private var Esquema = new StructType
  private var auxSchema = new StructType


  //Metodo para Obtener el objeto StructType de los campos
  def RDD_Obtener_Esquema():StructType = {

    //Devuelvo el resultado
    if (vValidacionCampos == true) {


      val ListaCampos = p_CadenaCampos.split(",")
      val ListaTipos = p_CadenaTipos.split(",")
      val ListaCombinada = ListaCampos zip ListaTipos
      var TipoField = org.apache.spark.sql.types.DataType

      for ((curr_campo, curr_tipo) <- ListaCombinada) {

        if (curr_tipo == "integer") {
          auxSchema = StructType(Array(StructField(curr_campo, IntegerType, true)))
        }
        if (curr_tipo == "boolean") {
          auxSchema = StructType(Array(StructField(curr_campo, BooleanType, true)))
        }
        if (curr_tipo == "long") {
          auxSchema = StructType(Array(StructField(curr_campo, LongType, true)))
        }
        if ((curr_tipo != "integer") || (curr_tipo != "long") || (curr_tipo == "boolean")) {
          auxSchema = StructType(Array(StructField(curr_campo, StringType, true)))
        }
        Esquema = StructType(auxSchema ++ Esquema)

      }
    }
    return Esquema;
  }






        var lCadenaCompuesta = new ListBuffer[String]()
        (p_CadenaCampos.split("\\|"), p_CadenaTipos.split("\\|")).zipped.map(_ + ":" + _ ) foreach schema.{add() }

        return lCadenaCompuesta.mkString(",").;

      }
    }
  }

  //método para calcular el número de veces que se repite un carácter en un String//método para calcular el número de veces que se repite un carácter en un String
  private def ContarCampos( vCadena:String, vSeparador: Char):Int = {
    var posicion = 0
    var contador = 1
    //se busca la primera vez que aparece
    posicion = vCadena.indexOf(vSeparador)
    while ( {
      posicion != -1
    }) { //mientras se encuentre el caracter
      contador += 1 //se cuenta

      //se sigue buscando a partir de la posición siguiente a la encontrada
      posicion = vCadena.indexOf(vSeparador, posicion + 1)
    }
    contador
  }

  //Metodo para Obtener el conjunto de campos 1,2,3...N numerado

  def ObtenerConjuntoCamposNumerado():String = {

    val vHasta = ContarCampos(p_CadenaCampos, p_Separador(0)) - 1
    var res=""

    for( vCont <- 0 to vHasta ){
      res += "row(" + vCont + ")"
      if ( vCont != vHasta ){
        res += ","
      }
    }
    return res
  }


  //case class SchemaMovements(COUNTRY_ID: String, MONTH_ID: String, CUSTOMER_ID: String, MSISDN_ID: String, SUBSCRIPTION_ID: String, ACTIVATION_DT: String, MOVEMENT_ID: String, MOVEMENT_DT: String, MOVEMENT_CHANNEL_ID: String, CAMPAIGN_ID: String, SEGMENT_CD: String, PRE_POST_ID: String, PREV_PRE_POST_ID: String, TARIFF_PLAN_ID: String, PREV_TARIFF_PLAN_ID: String, PROD_TYPE_CD: String, PORT_OP_CD: String)

}
