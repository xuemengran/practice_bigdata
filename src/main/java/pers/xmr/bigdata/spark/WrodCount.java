package pers.xmr.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author xmr
 * @date 2020/1/13 20:00
 * @description spark版本的WordCount
 */
public class WrodCount {
    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];
        SparkConf conf = new SparkConf();
        conf.setAppName("wordCount");
        conf.setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> wcRdd = jsc.textFile(inputPath);
        JavaRDD<String> stringJavaRDD = wcRdd.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).saveAsTextFile(outputPath);
    }
}
