package com.WuzzufJobAnalysis.job;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import java.util.ArrayList;

@Component
public class jobDAO {
    Dataset<Row> csvDataFrame;

    public jobDAO() {
        csvDataFrame = getDatasetFromCSV();
    }

    private Dataset<Row> getDatasetFromCSV(){
        SparkSession sparkSession;
        sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]")
                .getOrCreate ();
        return sparkSession.read().option("header", "true").csv ("src/main/resources/Wuzzuf_Jobs.csv");
    }
}
