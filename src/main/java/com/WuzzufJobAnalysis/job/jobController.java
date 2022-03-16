package com.WuzzufJobAnalysis.job;

import com.WuzzufJobAnalysis.job.jobDAO;
import com.WuzzufJobAnalysis.job.jobService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@RequestMapping("spark-context")
@Controller
public class jobController {

    @RequestMapping("read-csv")
    public ResponseEntity<String> getRowCount() {
        jobService service = new jobService();
//        LinkedHashMap<String, Integer> lhm =  service.getFeatureValuesCount("Title");
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
//        String html = new jobService().filterJobsByComp();
//        String html = new jobService().filterJobsByTitle();
        String html = new jobService().getMostskills();

       /*String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                String.format("<h3>%s</h3>", "Read csv..") +
//                String.format("<h4>Total records %d</h4>", dataset.count()) +
                String.format("<h5>Schema <br/> %s</h5> <br/> hashmap <br/>", lhm.values().toString());*/

        return ResponseEntity.ok(html);
    }
    @RequestMapping("get-most-skills")
    public ResponseEntity<String> getMostSkills() {
        jobService service = new jobService();
        Dataset<jobPOJO> dataset = new jobDAO().prepareData();

        String html = new jobService().getMostskills();
        return ResponseEntity.ok(html);
    }
    @RequestMapping("get-most-jobs")
    public ResponseEntity<String> getMostPopularJobs() {
        String html = new jobService().getMostPopularJobs();
        return ResponseEntity.ok(html);
    }
    @RequestMapping("get-most-areas")
    public ResponseEntity<String> getMostPopularAreas() {
        String html = new jobService().getMostPopularAreas();
        return ResponseEntity.ok(html);
    }
    @RequestMapping("schema")
    ResponseEntity<String> getSchema() {
        jobService service = new jobService();
        return ResponseEntity.ok(service.getSchema());
    }
//
//    @RequestMapping("kmeans")
//    Map<String, String> applyKmeans(){
//        return kmeans.applyKmeans();
//    }


    @RequestMapping("get-jobs_bycomp")
    public ResponseEntity<String> filterJobsByComp() {
        String html = new jobService().filterJobsByComp();
        return ResponseEntity.ok(html);
    }
    @RequestMapping("dataset-head")
    ResponseEntity<String> getDatasetHead() {
        jobService service = new jobService();
        return ResponseEntity.ok(service.getDatasetHead());
    }
    @RequestMapping("job-bycomp-chart")
    ResponseEntity<byte[]> jobbycompChart(){
        return getImage("Jobs Per Company");
    }

    @RequestMapping("most-jobs-chart")
    ResponseEntity<byte[]> mostjobsChart(){
        return getImage("Most_POPULAR_JOBS");
    }

    @RequestMapping("most-areas-chart")
        ResponseEntity<byte[]> mostareasChart(){
            return getImage("MOST_POPULAR_AREAS");
        }

    @RequestMapping("most-skill-chart")
    ResponseEntity<byte[]> mostskillChart(){
        return getImage("Most Wanted Skills");
    }

    ResponseEntity<byte[]> getImage(String chartName){
        String path= chartName+ ".jpg";
        ClassPathResource imgFile = new ClassPathResource(path);
        byte[] bytes = new byte[0];
        try {
            bytes = StreamUtils.copyToByteArray(imgFile.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseEntity
                .ok()
                .contentType(MediaType.IMAGE_JPEG)
                .body(bytes);
    }

}
