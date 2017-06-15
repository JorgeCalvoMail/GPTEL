
import org.apache.spark.SparkContext
val sc = new SparkContext()

var lista1 = sc.parallelize(Array("alfonso sonia ruben santiago","ignacio alberto juan"))
var lista2 = sc.parallelize(Array("rodrigo rocio samanta","lorena pepe david"))
var lista3 = Array(lista1.union(lista2))
var RDD1 = sc.parallelize(lista3.map(z => "Nombre: " + z))

RDD1.collect().foreach(println)
