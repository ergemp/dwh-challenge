package org.ergemp;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReportingJobMinio {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("ReportingJobMinio")
                //.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .master("local")
                .getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "minioadmin");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint","http://localhost:9000");
        spark.sparkContext().hadoopConfiguration().set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");

        Dataset<Row> category_dim = spark.read().option("header",true).csv("s3a://dwh-challenge/category_dim");
        Dataset<Row> customers_dim = spark.read().option("header",true).csv("s3a://dwh-challenge/customers_dim");
        Dataset<Row> date_dim = spark.read().option("header",true).csv("s3a://dwh-challenge/date_dim");
        Dataset<Row> location_dim = spark.read().option("header",true).csv("s3a://dwh-challenge/location_dim");
        Dataset<Row> order_items_fact = spark.read().option("header",true).csv("s3a://dwh-challenge/order_items_fact");
        Dataset<Row> orders_dim = spark.read().option("header",true).csv("s3a://dwh-challenge/orders_dim");
        Dataset<Row> payments_dim = spark.read().option("header",true).csv("s3a://dwh-challenge/payments_dim");
        Dataset<Row> products_dim = spark.read().option("header",true).csv("s3a://dwh-challenge/products_dim");

        category_dim.createOrReplaceTempView("category_dim");
        customers_dim.createOrReplaceTempView("customers_dim");
        date_dim.createOrReplaceTempView("date_dim");
        location_dim.createOrReplaceTempView("location_dim");
        order_items_fact.createOrReplaceTempView("order_items_fact");
        orders_dim.createOrReplaceTempView("orders_dim");
        payments_dim.createOrReplaceTempView("payments_dim");
        products_dim.createOrReplaceTempView("products_dim");

        //top5 products according to number of sales
        spark.sql(" select product_id, count(1) from order_items_fact " +
                    " group by product_id " +
                    " order by 2 desc " +
                    " limit 5 ")
                .show(false);

        //number of orders by year by month
        spark.sql("select " +
                    "  dd.year, " +
                    "  dd.month, " +
                    "  count(1) as number_of_orders " +
                    "from orders_dim od " +
                    "left join date_dim dd on (to_date(od.order_purchase_timestamp) = dd.ddate) " +
                    "group by dd.year, dd.month " +
                    "order by dd.year, dd.month")
                .show(false);

        //number of orders by city
        spark.sql("select " +
                    "   ld.location_city, " +
                    "   count(1) as number_of_orders " +
                    "from orders_dim od " +
                    "left join location_dim ld on (od.location_id = ld.location_id) " +
                    "group by ld.location_city order by 2 desc ")
                    .show(false);

        //total profit by year and month
        spark.sql("select " +
                    "   dd.year, " +
                    "   dd.month, " +
                    "   sum(cast(oidd.price as decimal(9,4))) as total_profit " +
                    "from order_items_fact oidd " +
                    "left join orders_dim od on (oidd.order_id = od.order_id) " +
                    "left join date_dim dd on (to_date(od.order_purchase_timestamp) = dd.ddate) " +
                    "group by dd.year, dd.month " +
                    "order by dd.year, dd.month")
                .show(false);

        //pct of orders that takes above 24h = 1 day
        spark.sql("select round(cast(late_orders as decimal(8,2))/ cast(total_orders as decimal(8,2)) * 100 , 2) as late_delivery_pct " +
                "from " +
                "( " +
                "select " +
                "   count(1) as total_orders, " +
                "   (select " +
                "       count(1) as late_orders " +
                "   from " +
                "   ( " +
                "       select " +
                "           order_status, " +
                "           order_purchase_timestamp as  order_purchase_timestamp, " +
                "           order_delivered_customer_date as  order_delivered_customer_date " +
                "       from orders_dim od " +
                "       ) sub " +
                "   where " +
                "   datediff(order_delivered_customer_date, order_purchase_timestamp) >= 1 " +
                "   and order_status='delivered' " +
                "   ) as late_orders " +
                "from orders_dim od " +
                ") sub")
                .show(false);

        //top5 product categories most commonly purchased by new customers
        spark.sql("select cd.category_name_english , count(1) total_sold from " +
                "   order_items_fact oif " +
                "   left join " +
                "   ( " +
                "       select " +
                "           od2.order_id " +
                "       from " +
                "           orders_dim od2 " +
                "       where (customer_id, to_timestamp(order_purchase_timestamp)) in " +
                "       ( " +
                "       select customers_dim.customer_unique_id, min(to_timestamp(orders_dim.order_purchase_timestamp)) as first_date  from customers_dim " +
                "           join orders_dim on (customers_dim.customer_id = orders_dim.customer_id) " +
                "       group by customers_dim.customer_unique_id " +
                "       ) " +
                "   ) first_purchases " +
                "   on (oif.order_id = first_purchases.order_id) " +
                "   left join products_dim pd on (oif.product_id = pd.product_id) " +
                "   left join category_dim cd on (pd.product_category_name = cd.category_name) " +
                "group by cd.category_name_english " +
                "order by 2 desc " +
                "limit 5").show(false);
    }
}
