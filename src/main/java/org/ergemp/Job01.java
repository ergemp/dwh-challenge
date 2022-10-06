package org.ergemp;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class Job01 {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("reco.challenge.Job01")
                .master("local")
                .getOrCreate();

        //open the local csv file
        String csvPath = "resources/data/products.csv";

        Dataset<Row> productsDf = spark.read().option("header",true).csv(csvPath);

        productsDf.printSchema();

        Properties cnnProps = new Properties();
        cnnProps.setProperty("driver", "org.postgresql.Driver");
        cnnProps.setProperty("user", "postgres");
        cnnProps.setProperty("password", "postgres");

        productsDf.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","products", cnnProps);


    }
}
