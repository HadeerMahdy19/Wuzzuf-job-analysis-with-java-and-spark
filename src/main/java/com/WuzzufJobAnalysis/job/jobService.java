package com.WuzzufJobAnalysis.job;

import org.apache.spark.sql.*;
import java.sql.Struct;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class jobService {

    Dataset<jobPOJO> jobData;
    SparkSession sparkSession;

    public jobService(){
        jobData = new jobDAO().prepareData();
        sparkSession=SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[4]").getOrCreate();
    }

    String getDatasetHead(){
        return jobData.showString(20, 20, true);
    }

    String getSchema(){
        return jobData.schema().toDDL();
    }

    String getSummary(){
        return jobData.summary("count").showString(8,10,true);
    }
//
//    String factorizeYearsExp() {
//        StringIndexerModel labelIndexer = new StringIndexer()
//                .setInputCol("YearsExp")
//                .setOutputCol("indexedLabel")
//                .fit(jobData);
//        return jobData.select("YearsExp").showString(8,10,true);
//    }




    /////////////////////////////////////////////////////////////////////////////////////////////////////


    // change sark version -> in pom file spark sql
    LinkedHashMap<String, Integer> getFeatureValuesCount(String colName){
        jobData.createOrReplaceTempView("Jobs");
        Dataset<Row> sql = sparkSession.sql("select "+colName+ ",CAST(count(*) AS INT) as count from Jobs group by "+colName+" order by count DESC");
        List<String> featureValues = sql.select(colName).as(Encoders.STRING()).collectAsList();
        List<Integer> count = sql.select("count").as(Encoders.INT()).collectAsList();
        return  createLinkedHashMap(featureValues, count);
    }

    LinkedHashMap<String, Integer> createLinkedHashMap( List<String> colValues,  List<Integer> count){
        LinkedHashMap<String, Integer> lhm = new LinkedHashMap<String, Integer>();
        for (int i = 0; i < colValues.size(); i++) {
                lhm.put(colValues.get(i), count.get(i));
        }
        return lhm;
    }


}
