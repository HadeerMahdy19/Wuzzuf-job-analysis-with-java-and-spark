package com.WuzzufJobAnalysis.job;

import org.apache.spark.sql.Dataset;

public class jobService {

    Dataset<jobPOJO> jobData;
    public jobService(){
        jobData = new jobDAO().prepareData();
    }


}
