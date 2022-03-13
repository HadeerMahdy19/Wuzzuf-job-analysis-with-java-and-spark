package com.WuzzufJobAnalysis.job;

import com.WuzzufJobAnalysis.job.jobDAO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.LinkedHashMap;

@RequestMapping("spark-context")
@Controller
public class jobController {

    @RequestMapping("read-csv")
    public ResponseEntity<String> getRowCount() {
        jobService service = new jobService();
        LinkedHashMap<String, Integer> lhm =  service.getFeatureValuesCount("Title");
        //System.out.println("*********************\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
        //System.out.println(lhm);
        //System.out.println("*********************\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
        Dataset<jobPOJO> dataset = new jobDAO().prepareData();
       // System.out.println("***********************************************************");
        //dataset.show();
        /*String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                String.format("<h3>%s</h3>", "Read csv..") +
//                String.format("<h4>Total records %d</h4>", dataset.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", dataset.schema().treeString()) +
                dataset.showString(20, 20, true);
        return ResponseEntity.ok(html);*/
        String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                String.format("<h3>%s</h3>", "Read csv..") +
//                String.format("<h4>Total records %d</h4>", dataset.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> hashmap <br/>", lhm.values().toString());
        return ResponseEntity.ok(html);
    }
}
