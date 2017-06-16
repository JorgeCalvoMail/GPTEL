import org.apache.spark.sql.types._

val p_CadenaCampos:String = "CAMPO1,CAMPO2,CAMPO3"
val p_CadenaTipos:String = "String,integer,String"
val p_Separador:String = ","

val ListaCampos = p_CadenaCampos.split(",")
val ListaTipos = p_CadenaTipos.split(",")
val ListaCombinada = (ListaCampos zip ListaTipos)


/*

var esquema = new StructType(
Array(StructField("campo1", StringType, true),
StructField("campo2", StringType, true),
StructField("campo3", StringType, true)))


val schema = new StructType()
  .add(StructField("Name", StringType, true))
  .add(StructField("Age", IntegerType, true))
val df = sqlContext.createDataFrame(rowsRdd, schema)
df.show()

*/

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

esquema.foreach(println)






