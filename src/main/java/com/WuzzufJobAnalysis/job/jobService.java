package com.WuzzufJobAnalysis.job;

import org.apache.spark.sql.*;

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.*;
import org.knowm.xchart.style.PieStyler;
import org.knowm.xchart.style.Styler;

public class jobService {

    Dataset<jobPOJO> jobData;
    SparkSession sparkSession;

    public jobService(){
        jobData = new jobDAO().prepareData();
        sparkSession=SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[4]").getOrCreate();
    }
    LinkedHashMap<String, Integer> getAllSkills(){
        jobData.createOrReplaceTempView("Jobs");
        Dataset<Row> sql = sparkSession.sql("select "+"Skills"+ ",CAST(count(*) AS INT) as count from Jobs group by "+"Skills"+" order by count DESC");
        List<String> skillValues = sql.select("Skills").as(Encoders.STRING()).collectAsList();
        List<Integer> count = sql.select("count").as(Encoders.INT()).collectAsList();
        return  createLinkedHashMap(skillValues, count);
    }
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

    public static LinkedHashMap<String, Integer> sortByValue(HashMap<String, Integer> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list =
                new LinkedList<Map.Entry<String, Integer> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2)
            {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // put data from sorted list to hashmap
        LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }
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
        for (int j = 0; j<keys.size() ; j++){
            sliceColors.add(new Color(objGenerator.nextInt(255), objGenerator.nextInt(255), objGenerator.nextInt(255)));
        }
        // converting sliceColors to array
        chart.getStyler ().setSeriesColors (sliceColors.toArray(new Color[sliceColors.size()]));



        for (int i=0; i<keys.size(); i++){

            chart.addSeries(keys.get(i), values.get(i));

        }

        // Display
        //new SwingWrapper<>(chart).displayChart();

        // Save the Visualization as a png
        try {
            String path="G:\\iti\\JavaProjectSRC\\";
            BitmapEncoder.saveBitmap(chart,path+title, BitmapEncoder.BitmapFormat.JPG);
        } catch (IOException e) {
            System.out.println(e);
        }

    }
    public static void barChart(List<String> keys, List<Integer> values, String title, String xlabel){
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
            String path="G:\\iti\\JavaProjectSRC\\";
            BitmapEncoder.saveBitmap(chart,path+title, BitmapEncoder.BitmapFormat.JPG);
//            BitmapEncoder.saveBitmap(chart,System.getProperty("user.dir")+"/Public/"+xlabel, BitmapEncoder.BitmapFormat.PNG);
        }
        catch (Exception e){
            System.out.println("NOT Found path");
        }
    }

    public static String getHtml(List<String> keys, List<Integer> values, String header, String colHeader){
//

        String output = "<html><head>\n"
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
        output+="   <title>Jops Per Company</title>\n" +
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
                "        <picture><img src=\"G:\\iti\\JavaProjectSRC\\Most Popular Area.jpg\"/></picture>\n" +
                "    </body>\n" +
                "</html>";
        return output;
    }




    public String filterJobsByComp(){
        // create hashmap to contain each company frequency
        HashMap<String, Integer> compFreq = new HashMap<>();
        for(jobPOJO job : jobData.collectAsList() ) {
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
            if (n >= limit){
                break;
            }
        }

        // Visualizing in bar chart
        pieChart(companies, counts, "Jops Per Company", 10);

        // Html output
        return getHtml(companies, counts, "Jops Per Company", "Company");
    }
    public String filterJobsByTitle(){
        // create hashmap to contain each title frequency
        HashMap<String, Integer> titleFreq = new HashMap<>();
        for(jobPOJO job : jobData.collectAsList()){
            if (titleFreq.containsKey(job.getTitle())){
                titleFreq.put(job.getTitle(), titleFreq.get(job.getTitle()) + 1);
            }
            else{
                titleFreq.put(job.getTitle(), 1);
            }
        }
        // Sorting to get high frequency titles at the beginning
        LinkedHashMap<String, Integer> sortedTitles = sortByValue(titleFreq);

        // Visualizing in bar chart
        int limit = 15 ;
        List<String> titles = new ArrayList<>();
        List<Integer> counts = new ArrayList<>();

        short n = 0;
        for (String i : sortedTitles.keySet()) {
            titles.add(i);
            counts.add(sortedTitles.get(i));
            n++;
            if (n >= limit){
                break;
            }
        }
        barChart(titles, counts, "Most Popular Titles", "Titles");

        // Html output
        return getHtml(titles, counts, "Most Popular Titles", "Title");
    }
    public String filterJobsByArea(){
        // create hashmap to contain each Area frequency
        HashMap<String, Integer> areaFreq = new HashMap<>();
        for(jobPOJO job : jobData.collectAsList()){
            if (areaFreq.containsKey(job.getLocation())){
                areaFreq.put(job.getLocation(), areaFreq.get(job.getLocation()) + 1);
            }
            else{
                areaFreq.put(job.getLocation(), 1);
            }
        }

        // Sorting to get high frequency Areas at the beginning
        LinkedHashMap<String, Integer> sortedArea = sortByValue(areaFreq);

        // Visualizing in bar chart
        int limit = 15 ;
        List<String> areas = new ArrayList<>();
        List<Integer> counts = new ArrayList<>();

        short n = 0;
        for (String i : sortedArea.keySet()) {
            areas.add(i);
            counts.add(sortedArea.get(i));
            n++;
            if (n >= limit){
                break;
            }
        }
        barChart(areas, counts, "Most Popular Area", "Area");

        // Html output
        return getHtml(areas, counts, "Most Popular Area", "Area");
    }
//    public String filterJobsBySkills() {
//        List<String[]> jobSkills = new ArrayList<>();
//        for (jobPOJO job : jobData.collectAsList()) {
//            jobSkills.add(job.getSkills());
//        }
//
//        // create hashmap to contain how many each skill has repeated:
//        HashMap<String, Integer> skillsFreq = new HashMap<>();
//        for (String[] skill : jobSkills) {
//            for (String x : skill) {
//                x = x.trim().replace("[", "").replace("]", "");
//                if (skillsFreq.containsKey(x)) {
//                    skillsFreq.put(x, skillsFreq.get(x) + 1);
//                } else {
//                    skillsFreq.put(x, 1);
//                }
//            }
//        }
//
//        // Sorting to get most frequent Skills at the beginning
//        LinkedHashMap<String, Integer> sortedSkills = sortByValue(skillsFreq);
//
//        // Visualizing in bar chart
//        int limit = 15;
//        List<String> skills = new ArrayList<>();
//        List<Integer> counts = new ArrayList<>();
//
//        short n = 0;
//        for (String i : sortedSkills.keySet()) {
//            skills.add(i);
//            counts.add(sortedSkills.get(i));
//            n++;
//            if (n >= limit) {
//                break;
//            }
//        }
//        barChart(skills, counts, "Most Popular Skills", "Skills");
//
//        // Html output
//        return getHtml(skills, counts, "Most Popular Skills", "Skill");
//    }
}
