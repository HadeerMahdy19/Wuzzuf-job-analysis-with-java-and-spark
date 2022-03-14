package com.WuzzufJobAnalysis.job;

import com.WuzzufJobAnalysis.job.jobDAO;
import com.WuzzufJobAnalysis.job.jobService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.LinkedHashMap;

@RequestMapping("spark-context")
@Controller
public class jobController {

//    final jobService service = new jobService();
    @RequestMapping("read-csv")

    public ResponseEntity<String> getRowCount() {
        jobService service = new jobService();
        LinkedHashMap<String, Integer> lhm =  service.getFeatureValuesCount("Title");
        //System.out.println("*********************\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
        //System.out.println(lhm);
        //System.out.println("*********************\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
        // System.out.println("***********************************************************");
        Dataset<jobPOJO> dataset = new jobDAO().prepareData();
        // System.out.println("***********************************************************");
        //dataset.show();
        /*String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                String.format("<h3>%s</h3>", "Read csv..") +
//                String.format("<h4>Total records %d</h4>", dataset.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", dataset.schema().treeString()) +
                dataset.showString(20, 20, true);
        return ResponseEntity.ok(html);*/
//        String html = new jobService().filterJobsByComp();
//        String html = new jobService().filterJobsByTitle();
        String html = new jobService().getMostPopularAreas();

       /*String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                String.format("<h3>%s</h3>", "Read csv..") +
//                String.format("<h4>Total records %d</h4>", dataset.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> hashmap <br/>", lhm.values().toString());*/


        String html = String.format("<h3>%s</h3>", "Read csv..") +
                String.format("<h5>Schema <br/> %s</h5> <br/> hashmap <br/>", lhm.values().toString());

        return ResponseEntity.ok(html);
    }

    @RequestMapping("dataset-head")
    ResponseEntity<String> getDatasetHead() {
        jobService service = new jobService();
        return ResponseEntity.ok(service.getDatasetHead());
    }

    @RequestMapping("schema")
    ResponseEntity<String> getSchema() {
        jobService service = new jobService();
        return ResponseEntity.ok(service.getSchema());
    }


    @RequestMapping("summary")
    ResponseEntity<String> getSummary() {
        jobService service = new jobService();
        return ResponseEntity.ok(service.getSummary());
    }

//    @RequestMapping("factorize-years-exp")
//    ResponseEntity<String> factorizeYearsExp() {
//        jobService service = new jobService();
//        return ResponseEntity.ok(service.factorizeYearsExp());
//    }

}
