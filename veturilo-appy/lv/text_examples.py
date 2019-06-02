code = {
    "geospark_prep": """
        sc.stop()
        import org.apache.spark.sql.SparkSession
        import org.datasyslab.geospark.spatialRDD._
        import org.datasyslab.geospark.enums._
        import org.datasyslab.geosparksql.utils._
        import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
        import org.apache.spark.serializer.KryoSerializer
        

        val spark = SparkSession.builder().appName("LivyTest").config("master", "local").config("spark.serializer", classOf[KryoSerializer].getName).config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).getOrCreate()
        GeoSparkSQLRegistrator.registerAll(spark)
        
        val municipalitiesPath = "/home/pawel/Desktop/livy_project/files/gminy.csv"
        val poiPath = "/home/pawel/Desktop/livy_project/files/pois.csv"
        
        val options = Map("delimiter"->"\\t", "header"->"false")
        
        val municipalities = spark.read.options(options).csv(municipalitiesPath)
        val pois = spark.read.options(options).csv(poiPath)
        
        municipalities.createOrReplaceTempView("m")
        pois.createOrReplaceTempView("p")
        
        val pGeom = spark.sql("select _c0 as id, ST_GeomFromWkt(_c1) as geom from m where _c1 is not null")
        val mGeom = spark.sql("select _c0 as id, ST_GeomFromWkt(_c2) as geom, _c1 as type from p where _c2 is not null")
        
        pGeom.repartition(100)
        mGeom.repartition(100)
        pGeom.cache().count()
        mGeom.cache().count()
        
        mGeom.createOrReplaceTempView("mGeom")
        pGeom.createOrReplaceTempView("pGeom") 
        

    """,

    "geospark_save": """
    result.saveAsTextFile("/home/pawel/Desktop/livy_project/files/{name}")
    """
}