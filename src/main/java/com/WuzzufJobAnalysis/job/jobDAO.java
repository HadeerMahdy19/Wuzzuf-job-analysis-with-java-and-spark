package com.WuzzufJobAnalysis.job;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;
import java.util.ArrayList;

@Component
public class jobDAO {
    public Dataset<jobPOJO> jobData;
    public jobDAO() {
        jobData = prepareData();
    }

    private Dataset<jobPOJO> prepareData() {
        Dataset<Row> rowDataset = getDatasetFromCSV();
        Dataset<jobPOJO> data = cleanData(rowDataset);
        return data;
    }

    private Dataset<Row> getDatasetFromCSV(){
        SparkSession sparkSession;
        sparkSession = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[4]")
                .getOrCreate();
        return sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
    }

    // remove null values and duplicates
    private Dataset<jobPOJO> cleanData(Dataset<Row> rowDataset) {
        return rowDataset.na().drop().dropDuplicates().as(Encoders.bean(jobPOJO.class));
    }
}
