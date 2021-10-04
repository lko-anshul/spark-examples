package org.anshul;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sparkContext.textFile("F:\\technical\\temp\\sample.log");
        JavaRDD<String> wordsFromFile = lines.filter(s -> s.contains("http://www.google.com/bot.html"));
        long numErrors = wordsFromFile.count();
        System.out.println(numErrors);
    }
}