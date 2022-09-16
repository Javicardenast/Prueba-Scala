object transposicion {


  /*En este ejercicio vamos a hacer un cruce sencillo, se llama transposicion.
  * Basicamente será mover una tabla vertical a horizontal*/

  /*
  Tenemos esta tabla:

  Indice  Indice_Fecha  Indice_Valor
  Ibex    26/08/2022    8063.9
  SyP     26/08/2022    4057.66
  FTSE    25/08/2022    7479.74

  Queremos obtener esta:

  Indice_Ibex_Fecha Indice_Ibex_Valor Indice_SyP_Fecha  Indice_SyP_Valor  Indice_FTSE_Fecha Indice_FTSE_Valor
      26/08/2022          8063.9         26/08/2022         4057.66           25/08/2022          7479.74

   */

  def main(args: Array[String]):Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder()
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate()

    val df= spark
      .read
      .format("csv")
      .option("header","true")
      .option("delimiter",";")
      .load(" Aplicamos la ruta del archivo")

    val ejercicio1 = df
      .groupBy()
      .pivot("Indice")
      .agg(first("Indice_Fecha")last("Indice_Valor"))
      .withColumnRenamed("Ibex_first(Indice_Fecha, false", "Indice_Ibex_Fecha")
      .withColumnRenamed("Ibex_last(Indice_Fecha, false", "Indice_Ibex_Valor")
      .withColumnRenamed("SyP_first(Indice_Fecha, false", "Indice_SyP_Fecha")
      .withColumnRenamed("SyP_last(Indice_Fecha, false", "Indice_SyP_Valor")
      .withColumnRenamed("FTSE_first(Indice_Fecha, false", "Indice_FTSE_Fecha")
      .withColumnRenamed("FTSE_last(Indice_Fecha, false", "Indice_FTSE_Valor")

  }
}
