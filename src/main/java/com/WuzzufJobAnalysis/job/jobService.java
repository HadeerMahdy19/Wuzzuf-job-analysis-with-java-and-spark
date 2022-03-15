package com.WuzzufJobAnalysis.job;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.*;
import org.knowm.xchart.style.PieStyler;
import org.knowm.xchart.style.Styler;

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.*;

public class jobService {

    Dataset<jobPOJO> jobData;
    SparkSession sparkSession;

    public jobService() {        jobData = new jobDAO().prepareData();
        sparkSession = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[4]").getOrCreate();
    }

    // this is a generic method that gets the counts of elements according to a specific feature using sql (group by)
    public LinkedHashMap<String, Integer> getFeatureValuesCount(String colName) {
        jobData.createOrReplaceTempView("Jobs");
        Dataset<Row> sql = sparkSession.sql("select " + colName + ",CAST(count(*) AS INT) as count from Jobs group by " + colName + " order by count DESC");
        List<String> featureValues = sql.select(colName).as(Encoders.STRING()).collectAsList();
        List<Integer> count = sql.select("count").as(Encoders.INT()).collectAsList();
        return createLinkedHashMap(featureValues, count);
    }


    public String getMostPopularJobs() {
        // get a hashmap of title(jobs name) and its count
        LinkedHashMap<String, Integer> mostPopularJobs = getFeatureValuesCount("Title");

        //split the hashmap into 2 lists using stream and get the first 10 elements
        List<String> titles = mostPopularJobs.keySet().stream().limit(10).collect(Collectors.toList());
        List<Integer> count = mostPopularJobs.values().stream().limit(10).collect(Collectors.toList());

        // create a bar chart for the results we got and save it in resources
        barChart(titles, count, "MOST_POPULAR_JOBS", "jobs");

        // present the result in an Html output structure
        String imgPath = "\\charts\\MOST_POPULAR_JOBS.jpg";
        return getHtml(titles, count, "MOST POPULAR JOBS", "Title", imgPath);


    }

    public String getMostPopularAreas() {
        // get a hashmap of area (location) and its count
        LinkedHashMap<String, Integer> mostPopularAreas = getFeatureValuesCount("Location");

        //split the hashmap into 2 lists using stream and get the first 10 elements
        List<String> areas = mostPopularAreas.keySet().stream().limit(10).collect(Collectors.toList());
        List<Integer> count = mostPopularAreas.values().stream().limit(10).collect(Collectors.toList());

        // create a bar chart for the results we got and save it in resources
        barChart(areas, count, "MOST_POPULAR_AREAS", "Areas");

        // present the result in an Html output structure
        String imgPath = "\\charts\\MOST_POPULAR_AREAS.jpg";
        imgPath = "D:\\java2\\_javaProject\\src\\main\\resources\\charts\\Jobs Per Company.jpg";
        return getHtml(areas, count, "MOST POPULAR AREAS", "Location", imgPath);
    }

    public String filterJobsByComp() {
        // create hashmap to contain each company frequency
        HashMap<String, Integer> compFreq = new HashMap<>();
        for (jobPOJO job : jobData.collectAsList()) {
            if (compFreq.containsKey(job.getCompany())) {
                compFreq.put(job.getCompany(), compFreq.get(job.getCompany()) + 1);
            } else {
                compFreq.put(job.getCompany(), 1);
            }
        }
        // Sorting to get high frequency company at the beginning
        LinkedHashMap<String, Integer> sortedComp = sortByValue(compFreq);

        int limit = 15;
        List<String> companies = new ArrayList<>();
        List<Integer> counts = new ArrayList<>();

        short n = 0;
        for (String i : sortedComp.keySet()) {
            companies.add(i);
            counts.add(sortedComp.get(i));
            n++;
            if (n >= limit) {
                break;
            }
        }

        // Visualizing in bar chart
        pieChart(companies, counts, "Jobs Per Company", 10);
        String imgPath = "";
        // Html output
        return getHtml(companies, counts, "Jobs Per Company", "Company", imgPath);
    }


    ////////////////////// ***********  helping methods ******* ///////////////////////////////

    // merging the 2 lists of keys and values into one linked hashmap
    LinkedHashMap<String, Integer> createLinkedHashMap(List<String> colValues, List<Integer> count) {
        LinkedHashMap<String, Integer> lhm = new LinkedHashMap<String, Integer>();
        for (int i = 0; i < colValues.size(); i++) {
            lhm.put(colValues.get(i), count.get(i));
        }
        return lhm;
    }

    public static LinkedHashMap<String, Integer> sortByValue(HashMap<String, Integer> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // put data from sorted list to linked hashmap
        LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    ////////////////////****** drawing charts methods ******////////////////////////////

    public static void pieChart(List<String> keys, List<Integer> values, String title, int limit) {
        // Create Chart
        PieChart chart = new PieChartBuilder().width(800).height(600).title(title).build();
        // Customize Chart
        chart.getStyler().setLegendVisible(false);
        chart.getStyler().setAnnotationType(PieStyler.AnnotationType.LabelAndPercentage);
        chart.getStyler().setAnnotationDistance(1.15);
        chart.getStyler().setPlotContentSize(.7);
        chart.getStyler().setStartAngleInDegrees(90);

        List<Color> sliceColors = new ArrayList<Color>();
        Random objGenerator = new Random();
        for (int j = 0; j < keys.size(); j++) {
            sliceColors.add(new Color(objGenerator.nextInt(255), objGenerator.nextInt(255), objGenerator.nextInt(255)));
        }
        // converting sliceColors to array
        chart.getStyler().setSeriesColors(sliceColors.toArray(new Color[sliceColors.size()]));

        for (int i = 0; i < keys.size(); i++) {

            chart.addSeries(keys.get(i), values.get(i));

        }

        // Display
        //new SwingWrapper<>(chart).displayChart();

        // Save the Visualization as a png
        try {
            String path = "D:\\java2\\_javaProject\\src\\main\\resources\\charts\\";
            BitmapEncoder.saveBitmap(chart, path + title, BitmapEncoder.BitmapFormat.JPG);
        } catch (IOException e) {
            System.out.println("NOT Found path");
        }

    }

    public static void barChart(List<String> keys, List<Integer> values, String title, String xlabel) {
        // Create Chart
        CategoryChart chart =
                new CategoryChartBuilder()
                        .width(1000)
                        .height(800)
                        .title(title)
                        .xAxisTitle(xlabel)
                        .yAxisTitle("Counts")
                        .build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setPlotGridLinesVisible(false);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setXAxisLabelRotation(90);

        // Series
        chart.addSeries(xlabel, keys, values);

        // Display
        //new SwingWrapper<>(chart).displayChart();

        // Save the Visualization as a png
        try {
            String path = "D:\\java2\\_javaProject\\src\\main\\resources\\charts\\";
            BitmapEncoder.saveBitmap(chart, path + title, BitmapEncoder.BitmapFormat.JPG);
//            BitmapEncoder.saveBitmap(chart,System.getProperty("user.dir")+"/Public/"+xlabel, BitmapEncoder.BitmapFormat.PNG);
        } catch (Exception e) {
            System.out.println("NOT Found path");
        }
    }


    /////////////////////////// ********** html code structure method ****/////////////////////////////////
    public static String getHtml(List<String> keys, List<Integer> values, String header, String colHeader, String path) {
//
        return String.format("<img src=\"%s\">", "/" + path);

       /* String output = "<html><head>\n"
                + "<style>\n" +
                "table {\n" +
                "  border-collapse: collapse;\n" +
                "  width: 25%;\n" +
                "}\n" +
                "\n" +
                "th, td {\n" +
                "  text-align: left;\n" +
                "  padding: 8px;\n" +
                "}\n" +
                "\n" +
                "tr:nth-child(even){background-color: #f2f2f2}\n" +
                "\n" +
                "th {\n" +
                "  background-color: #04AA6D;\n" +
                "  color: white;\n" +
                "}\n" +
                "</style>\n" +
                "</head><body>";
        output += "<h1>"+header+"</h1><table>";
        output += "<tr><th>"+colHeader+"</th><th>Counts</th></tr>";
        for (int i=0; i<keys.size(); i++){
            output += "<tr><td>"+keys.get(i)+"</td><td>"+values.get(i)+"</td></tr>";
        }
        output += "</table></body>";
        output+="   <title>Jobs Per Company</title>\n" +
                "    <body>\n" +
//                "        <div><\\charts\\MOST_POPULAR_AREAS.jpg\"></div>\n" +
               // "<div><img src = \"" + path + "\"" + "/></div>"+
                "<img src = \"try.png\" />"+
                //"        <div><img src=" + path + "></div>\n" +
                "    </body>\n" +
                "</html>";
        return output;
    }*/
      /*  String output = "<html>";
        output+="   <title>lmage</title>\n" +
                "        <meta charset=\"UTF-8\">\n" +
                "        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "        <style>\n" +
                "                img {\n" +
                "            border: 1px solid #ddd;\n" +
                "            border-radius: 4px;\n" +
                "            padding: 5px;\n" +
                "            width: 800px;\n" +
                "            height: 600px;\n" +
                "            max-width: 100%;\n" +
                "            height: auto;\n" +
                "            margin-left: 300px;\n" +
                "        }\n" +
                "        </style>\n" +
                "    </head>\n" +
                "    <body>\n" +
                "        <div><img src=\"D:\\java2\\_javaProject\\src\\main\\resources\\charts\\MOST_POPULAR_AREAS.jpg\"></div>\n" +
                "    </body>\n" +
                "</html>";
        return output;
    }*/
    }
}
