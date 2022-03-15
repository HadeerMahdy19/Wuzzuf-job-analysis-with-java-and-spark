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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.LinkedHashMap;

@RequestMapping("spark-context")
@Controller
@RestController
public class jobController {

    @GetMapping("read-csv")
    ResponseEntity<byte[]> getAreasChart(){
        return getImage("MOST_POPULAR_AREAS");
    }

    ResponseEntity<byte[]> getImage(String chartName){

        String path="charts/"+ chartName+ ".jpg";
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
//    @RequestMapping("read-csv")
//    public ResponseEntity<String> getRowCount() {
//        //jobService service = new jobService();
//      //  LinkedHashMap<String, Integer> lhm =  service.getFeatureValuesCount("Title");
//        //System.out.println("*********************\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
//        //System.out.println(lhm);
//        //System.out.println("*********************\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ");
//    //    Dataset<jobPOJO> dataset = new jobDAO().prepareData();
//        // System.out.println("***********************************************************");
//        //dataset.show();
//        /*String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
//                String.format("<h3>%s</h3>", "Read csv..") +
////                String.format("<h4>Total records %d</h4>", dataset.count()) +
//                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", dataset.schema().treeString()) +
//                dataset.showString(20, 20, true);
//        return ResponseEntity.ok(html);*/
//  //      String html = new jobService().filterJobsByComp();
//        //String html = new jobService().getMostPopularJobs();
//        String html = new jobService().getMostPopularAreas();
//
//       /*String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
//                String.format("<h3>%s</h3>", "Read csv..") +
////                String.format("<h4>Total records %d</h4>", dataset.count()) +
//                String.format("<h5>Schema <br/> %s</h5> <br/> hashmap <br/>", lhm.values().toString());*/
//
//        return ResponseEntity.ok(html);
//    }

// this was the last try
//    @RequestMapping("read-csv")
//    public ModelAndView analysis() throws IOException {
//
//        ModelAndView model = new ModelAndView("analysis.html");
//        return model;
//
//    }

}
