package com.teknologibigdata.spark;
//WordCount.java

import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount {

    private static final Pattern SPACE = Pattern.compile(" ");
    public List<String> enStopwords = new ArrayList<>();
    public final SparkSession spark;

    public WordCount() throws IOException {
        spark = SparkSession
                .builder()
                .appName("WordCount")
                .master("local[1]")//for local standalone execution
                .getOrCreate();

        readStopwords();
    }

    private void readStopwords() throws IOException {
        BufferedReader bfr = new BufferedReader(
                new InputStreamReader(
                        WordCount.class.getResourceAsStream("/en_stopwords.txt")
                )
        );

        String line = null;
        while ((line = bfr.readLine()) != null) {
            enStopwords.add(line);
        }
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <inputFile> <outputFile>");
            System.exit(1);
        }

        WordCount wc = new WordCount();
        List<String> stopwords = wc.enStopwords;
        Dataset<String> textDf = wc.spark.read().textFile(args[0]);
        textDf.show(10);

        Dataset<Row> wordCount = textDf
                .flatMap(line -> Arrays.asList(SPACE.split(line.replaceAll("\\W", " "))).iterator(), Encoders.STRING())
                .filter(str -> !str.isEmpty())
                .filter(str->!stopwords.contains(str))
                .map(word -> new Tuple2<>(word.toLowerCase(), 1L), Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
                .toDF("word", "one")
                .groupBy("word")
                .sum("one").orderBy(new Column("sum(one)").desc())
                .withColumnRenamed("sum(one)", "count");

        wordCount.show(10);
        wordCount.write().format("csv").save(args[1]);
    }
}
