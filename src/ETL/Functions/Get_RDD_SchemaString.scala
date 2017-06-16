
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

class Generic_RDD_SchemaString () {

  //Metodo para Obtener el objeto StructType de los campos
  def RDD_Obtener_Esquema(p_CadenaCampos:String = "", p_CadenaTipos:String = "", p_Separador:String = ""):StructType = {

     var Esquema = new StructType
     var auxSchema = new StructType


      val ListaCampos = p_CadenaCampos.split(",")
      val ListaTipos = p_CadenaTipos.split(",")
      val ListaCombinada = ListaCampos zip ListaTipos
      var TipoField = org.apache.spark.sql.types.DataType
      var esquema = new StructType()

      for ((curr_campo, curr_tipo) <- ListaCombinada) {

        if (curr_tipo == "integer") {
          var auxSchemaInt = StructType(Array(StructField(curr_campo, IntegerType)))
          esquema = StructType(esquema ++ auxSchemaInt)
        }
        if (curr_tipo == "boolean") {
          var auxSchemaBool = StructType(Array(StructField(curr_campo, BooleanType)))
          esquema = StructType(esquema ++ auxSchemaBool)
        }
        if (curr_tipo == "long") {
          var auxSchemaLong = StructType(Array(StructField(curr_campo, LongType)))
          esquema = StructType(esquema ++ auxSchemaLong)
        }
        if ((curr_tipo != "integer") && (curr_tipo != "long") && (curr_tipo != "boolean")) {
          println("entrafin")
          var auxSchemaString = StructType(Array(StructField(curr_campo, StringType)))
          esquema = StructType(esquema ++ auxSchemaString)
        }

      }

    return Esquema;
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

}
