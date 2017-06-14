import org.apache.spark.SparkContext

val sc = new SparkContext()
val sqlContext= new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._


//                                                      NECESITAMOS QUE LA RUTA LA COJA POR PARAMETRO
val fich_movements = sc.textFile("hdfs://nd3.hgdu.com:8020/Ficheros_LTV/CO/201608/MOVEMENTS/CO_MOVEMENTS_01_201608")


//Quitamos la cabecera para quedarnos con los datos     NECESITAMOS QUE EN CASO DE QUE CONTIENE CABECERA SEA TRUE SE OMITA LA PRIMERA LINEA
val data_fich_movements = fich_movements.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

//Creamos un Schema con la estructura del fichero:      NECESITAMOS SE GENERE EL ESQUEMA CON LAS COLUMNAS DINAMICAMENTE
//                                                      Tenemos que disponer de una funcion que nos convierta de una tupla de campos a una en formato texto concatenado el tipo de dato

case class SchemaMovements(COUNTRY_ID: String, MONTH_ID: String, CUSTOMER_ID: String, MSISDN_ID: String, SUBSCRIPTION_ID: String, ACTIVATION_DT: String, MOVEMENT_ID: String, MOVEMENT_DT: String, MOVEMENT_CHANNEL_ID: String, CAMPAIGN_ID: String, SEGMENT_CD: String, PRE_POST_ID: String, PREV_PRE_POST_ID: String, TARIFF_PLAN_ID: String, PREV_TARIFF_PLAN_ID: String, PROD_TYPE_CD: String, PORT_OP_CD: String)
val movements = data_fich_movements.map(_.split("\\|")).map(row => SchemaMovements(row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9), row(10), row(11), row(12), row(13), row(14), row(15), row(16))).toDF()








//ELIMINAMOS LA TABLA
sqlContext.sql("use ltv_stg_database")
sqlContext.sql("DROP TABLE IF EXISTS ltv_stg_database.STGTBL_LTV_CO_MOVEMENTS")

//LA CREAMOS
movements.registerTempTable("fich_movements")










val results = sqlContext.sql("CREATE TABLE ltv_stg_database.STGTBL_LTV_CO_MOVEMENTS stored as orc as select * from fich_movements")

//Nos vamos al paso de ltv_raw_database
sqlContext.sql("use ltv_raw_database")

//PASO 1: HACEMOS DROP DE LA PARTICION DE LA TABLA RAW RAW_LTV_CO_MOVEMENTS
sqlContext.sql("ALTER TABLE ltv_raw_database.RAW_LTV_CO_MOVEMENTS DROP IF EXISTS PARTITION (MONTH_ID='201608')")
//PASO 2: CARGAMOS LA INFORMACION DESDE LA TEMPORAL
sqlContext.sql("INSERT OVERWRITE TABLE ltv_raw_database.RAW_LTV_CO_MOVEMENTS PARTITION (MONTH_ID='201608') SELECT country_id, customer_id, msisdn_id, subscription_id, activation_dt, movement_id, movement_dt, movement_channel_id, campaign_id, segment_cd, pre_post_id, prev_pre_post_id, tariff_plan_id, prev_tariff_plan_id, prod_type_cd, port_op_cd FROM ltv_stg_database.STGTBL_LTV_CO_MOVEMENTS WHERE MONTH_ID='201608'")

//ltv_dwu_database:
//Para movements vamos a necesitar generar el autonumerico de 2 campos CUSTOMER_ID --> CUSTOMER_GDU y SUBSCRIPTION_ID --> SUBSCRIPTION_GDU
//Para cada Identificador-Pais tenemos una tabla LTV_{PAIS}_DWA_{IDENTIFICADOR}_F que va a contener el Identificador y su equivalente autonumerico
//Lo primero que tenemos que saber es el máximo valor que tiene el identificador_gdu en dicha tabla, con lo que vamos a sacar en una variable el máximo para ambos identificadores
sqlContext.sql("use ltv_dwu_database")
val maxMSISDNGdu = sqlContext.sql("SELECT NVL(MAX(msisdn_gdu),0) from ltv_dwu_database.LTV_CO_DWA_MSISDN_F")
val maxSubscriptionGdu = sqlContext.sql("SELECT NVL(MAX(subscription_gdu),0) from ltv_dwu_database.LTV_CO_DWA_SUBSCRIPTION_F")
//Ahora que ya tenemos los dos máximos lo que vamos a tener es que sacar los pares IDENTIFICADORES encriptados-IDENTIFICADORES AUTONUMERICOS que no se encuentran en la tabla REAL DE AUTONUMERICO
sqlContext.sql("use ltv_stg_database")
sqlContext.sql("DROP TABLE IF EXISTS ltv_stg_database.TEMPORAL_AUTONUM_MSISDN_NUEVOS")
sqlContext.sql("CREATE TABLE ltv_stg_database.TEMPORAL_AUTONUM_MSISDN_NUEVOS AS SELECT A.MSISDN_ID, " + maxMSISDNGdu.first.getLong(0) +  " + ROW_NUMBER() OVER (PARTITION BY TOTAL ORDER BY TOTAL) AS MSISDN_GDU FROM (SELECT DISTINCT M.MSISDN_ID, 1 AS TOTAL FROM ltv_stg_database.STGTBL_LTV_CO_MOVEMENTS M LEFT JOIN ltv_dwu_database.LTV_CO_DWA_MSISDN_F F ON M.MSISDN_ID=F.msisdn_id WHERE F.msisdn_id IS NULL) A")
sqlContext.sql("DROP TABLE IF EXISTS ltv_stg_database.TEMPORAL_AUTONUM_SUBSCRIPTION_NUEVOS")
sqlContext.sql("CREATE TABLE ltv_stg_database.TEMPORAL_AUTONUM_SUBSCRIPTION_NUEVOS AS SELECT A.SUBSCRIPTION_ID, " + maxSubscriptionGdu.first.getLong(0) + " + ROW_NUMBER() OVER (PARTITION BY TOTAL ORDER BY TOTAL) AS SUBSCRIPTION_GDU FROM (SELECT DISTINCT M.SUBSCRIPTION_ID, 1 AS TOTAL FROM ltv_stg_database.STGTBL_LTV_CO_MOVEMENTS M LEFT JOIN  ltv_dwu_database.LTV_CO_DWA_SUBSCRIPTION_F F ON M.sUBSCRIPTION_ID=F.SUBSCRIPTION_ID WHERE F.SUBSCRIPTION_ID IS NULL) A")

//Los insertamos en la tabla real de autonumerico _F
sqlContext.sql("use ltv_dwu_database")
sqlContext.sql("insert into table ltv_dwu_database.LTV_CO_DWA_MSISDN_F select MSISDN_ID, MSISDN_GDU from ltv_stg_database.TEMPORAL_AUTONUM_MSISDN_NUEVOS")
sqlContext.sql("insert into table ltv_dwu_database.LTV_CO_DWA_SUBSCRIPTION_F select SUBSCRIPTION_ID, SUBSCRIPTION_GDU from  ltv_stg_database.TEMPORAL_AUTONUM_SUBSCRIPTION_NUEVOS")

//Ya con la tabla de autonumerico lista con todos los ID y GDU pasamos a cargar la particion de la tabla DWU con los identificadores autonuméricos exclusivamente...
sqlContext.sql("ALTER TABLE ltv_dwu_database.ltv_co_dwu_movements_dpp DROP IF EXISTS PARTITION (MONTH_ID=201608)")
sqlContext.sql("INSERT INTO TABLE ltv_dwu_database.ltv_co_dwu_movements_dpp PARTITION (MONTH_ID=201608) SELECT B.MSISDN_GDU, A.ACTIVATION_DT, A.MOVEMENT_ID, A.MOVEMENT_DT, A.MOVEMENT_CHANNEL_ID, A.CAMPAIGN_ID, A.SEGMENT_CD, A.PRE_POST_ID, A.PREV_PRE_POST_ID, A.TARIFF_PLAN_ID, A.PREV_TARIFF_PLAN_ID, A.PROD_TYPE_CD, A.PORT_OP_CD, A.CUSTOMER_ID, C.SUBSCRIPTION_GDU, A.COUNTRY_ID, CURRENT_DATE FEC_CARGA FROM ltv_raw_database.raw_ltv_co_movements A LEFT JOIN ltv_dwu_database.ltv_co_dwa_msisdn_f B ON A.MSISDN_ID=B.msisdn_id LEFT JOIN ltv_dwu_database.ltv_co_dwa_subscription_f C ON A.SUBSCRIPTION_ID=C.SUBSCRIPTION_ID")