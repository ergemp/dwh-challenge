package org.ergemp;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class EtlJobPostgres {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("EtlJobPostgres")
                .master("local")
                .getOrCreate();


        Properties cnnProps = new Properties();
        cnnProps.setProperty("driver", "org.postgresql.Driver");
        cnnProps.setProperty("user", "postgres");
        cnnProps.setProperty("password", "postgres");

        //open the local csv file
        String productsPath = "resources/data/products.csv";
        String ordersPath = "resources/data/orders.csv";
        String orderItemsPath = "resources/data/order_items.csv";
        String customersPath = "resources/data/customers.csv";
        String paymentsPath = "resources/data/payments.csv";
        String categoryNamePath = "resources/data/product_category_name.csv";

        Dataset<Row> productsDf = spark.read().option("header",true).csv(productsPath);
        Dataset<Row> ordersDf = spark.read().option("header",true).csv(ordersPath);
        Dataset<Row> orderItemsDf = spark.read().option("header",true).csv(orderItemsPath);
        Dataset<Row> customersDf = spark.read().option("header",true).csv(customersPath);
        Dataset<Row> paymentsDf = spark.read().option("header",true).csv(paymentsPath);
        Dataset<Row> categoryDf = spark.read().option("header",true).csv(categoryNamePath);

        productsDf.printSchema();
        System.out.println(productsDf.count());
        ordersDf.printSchema();
        System.out.println(ordersDf.count());
        orderItemsDf.printSchema();
        System.out.println(orderItemsDf.count());
        customersDf.printSchema();
        System.out.println(customersDf.count());
        paymentsDf.printSchema();
        System.out.println(paymentsDf.count());
        categoryDf.printSchema();
        System.out.println(categoryDf.count());

        //creating location_dim
        customersDf
                .withColumn("location_id", md5(expr("customer_city || customer_state || customer_zip_code_prefix"))).as("location_id")
                .select(
                        col("location_id"),
                        col("customer_city").as("location_city"),
                        col("customer_state").as("location_state"),
                        col("customer_zip_code_prefix").as("location_zip_code_prefix"))
                .distinct()
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","location_dim", cnnProps)
        //        .show(false)
        ;

        //creating customers_dim
        customersDf
                .withColumn("location_id", md5(expr("customer_city || customer_state || customer_zip_code_prefix"))).as("location_id")
                .select(
                        col("location_id"),
                        col("customer_id"),
                        col("customer_unique_id"))
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","customers_dim", cnnProps)

        //        .show(false)
        ;

        //creating orders_dim
        ordersDf
                .join(customersDf, customersDf.col("customer_id").equalTo(ordersDf.col("customer_id")),"left")
                .withColumn( "location_id", md5(expr("customer_city || customer_state || customer_zip_code_prefix"))).as("location_id")
                .select(col("order_id"),
                        col("location_id"),
                        col("index"),
                        ordersDf.col("customer_id"),
                        col("order_status"),
                        col("order_purchase_timestamp"),
                        col("order_approved_at"),
                        col("order_delivered_carrier_date"),
                        col("order_delivered_customer_date"),
                        col("order_estimated_delivery_date"))
                .distinct()
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","orders_dim", cnnProps)
        //        .show(false)
        ;

        //creating payments_dim
        paymentsDf
                .select(col("order_id"),
                        col("payment_sequential"),
                        col("payment_type"),
                        col("payment_installments"),
                        col("payment_value"))
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","payments_dim", cnnProps)
        //        .show(false)
        ;

        //creating products_dim
        productsDf
                .select(
                        col("product_id"),
                        col("product_category_name"),
                        col("product_name_lenght").as("product_name_length"),
                        col("product_description_lenght").as("product_description_length"),
                        col("product_photos_qty"),
                        col("product_weight_g"),
                        col("product_length_cm"),
                        col("product_height_cm"),
                        col("product_width_cm"))
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","products_dim", cnnProps)
        //        .show(false)
        ;

        //creating category_dim
        categoryDf
                .select(
                        col("product_category_name").alias("category_name"),
                        col("product_category_name_english").alias("category_name_english"))
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","category_dim", cnnProps)
        //        .show(false)
        ;


        //creating date_dim
        spark.sql(" select " +
                        " ddate, " +
                        " year(ddate) as year," +
                        " month(ddate) as month, " +
                        " day(ddate) as day, " +
                        " dayofweek(ddate) as weekday, " +
                        " dayofyear(ddate) as yearday, " +
                        " weekofyear(ddate) as week, " +
                        " quarter(ddate) as quarter " +
                        " from " +
                        " ( " +
                        " select " +
                        " explode(sequence(to_date('2000-01-01'), to_date('2030-01-01'), interval 1 day)) as ddate" +
                        " ) ")
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","date_dim", cnnProps)
        //        .show(100,false)
        ;


        //creating order_items_fact
        orderItemsDf
                .join(ordersDf,orderItemsDf.col("order_id").equalTo(ordersDf.col("order_id")),"left")
                .join(customersDf,ordersDf.col("customer_id").equalTo(customersDf.col("customer_id")),"left")
                .select(orderItemsDf.col("order_id"),
                        orderItemsDf.col("order_item_id"),
                        orderItemsDf.col("product_id"),
                        orderItemsDf.col("seller_id"),
                        md5(expr("customer_city || customer_state || customer_zip_code_prefix")).as("location_id"),
                        orderItemsDf.col("shipping_limit_date"),
                        orderItemsDf.col("price"),
                        orderItemsDf.col("freight_value"))
                .distinct()
                .write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","order_items_fact", cnnProps)
        //        .show(false)
        ;
        ;

        //productsDf.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","products", cnnProps);

    }
}
