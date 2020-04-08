package pers.xmr.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author xmr
 * @date 2019/4/29 10:02
 * @description spark转换操作RDD
 */
public class SparkTransformRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("transform rdd test");
        sparkConf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> javaRDD1 = jsc.parallelize(Arrays.asList(1,2,2,3,4,5,6,7,8,9));
        JavaRDD<Integer> javaRDD2 = jsc.parallelize(Arrays.asList(2,4,6,8,10));
        JavaRDD<String> javaRDD3 = jsc.parallelize(Arrays.asList("1,3,5,7,9","2,4,6,8,10"));
        mapTest(javaRDD1);
        filterTest(javaRDD1);
        flatMapTest(javaRDD3);
        distinctTest(javaRDD1);
        unionTest(javaRDD1, javaRDD2);
        cartesionTest(javaRDD1, javaRDD2);
        intersectionTest(javaRDD1, javaRDD2);
        mapToPairTest(javaRDD3);
        substartTest(javaRDD1, javaRDD2);
        sampleTest(javaRDD1);
    }

    private static void sampleTest(JavaRDD<Integer> javaRDD1) {
        JavaRDD<Integer> sample = javaRDD1.sample(false, 0.1);
        resultCollect(sample);
    }

    private static void substartTest(JavaRDD<Integer> javaRDD1, JavaRDD<Integer> javaRDD2) {
        JavaRDD<Integer> subtract = javaRDD1.subtract(javaRDD2);
        resultCollect(subtract);
    }

    private static void mapToPairTest(JavaRDD<String> javaRDD3) {
    }

    private static void intersectionTest(JavaRDD<Integer> javaRDD1, JavaRDD<Integer> javaRDD2) {
        JavaRDD<Integer> intersection = javaRDD1.intersection(javaRDD2);
        resultCollect(intersection);
    }

    private static void cartesionTest(JavaRDD<Integer> javaRDD1, JavaRDD<Integer> javaRDD2) {
        JavaPairRDD<Integer, Integer> cartesian = javaRDD1.cartesian(javaRDD2);
        List<Tuple2<Integer, Integer>> collects = cartesian.collect();
        for (Tuple2<Integer, Integer> collect: collects) {
            System.out.print(collect._1 + "," + collect._2 +" ");
        }
        System.out.println();
    }

    private static void unionTest(JavaRDD<Integer> javaRDD1, JavaRDD<Integer> javaRDD2) {
        JavaRDD<Integer> union = javaRDD1.union(javaRDD2);
        resultCollect(union);
    }

    private static void distinctTest(JavaRDD<Integer> javaRDD1) {
        JavaRDD<Integer> distinct = javaRDD1.distinct(1);
        resultCollect(distinct);
    }

    private static void flatMapTest(JavaRDD<String> javaRDD3) {
        JavaRDD<String> javaRDD = javaRDD3.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] strings = s.split(",");
                List<String> list = new ArrayList<String>(Arrays.asList(strings));
                return list.iterator();
            }
        });
        resultCollect(javaRDD);
    }

    private static void filterTest(JavaRDD<Integer> javaRDD1) {
        JavaRDD<Integer> filter = javaRDD1.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 != 0;
            }
        });
        List<Integer> collect = filter.collect();
        resultCollect(filter);
    }

    private static void resultCollect(JavaRDD filter) {
        List collect = filter.collect();
        for (int i = 0; i < collect.size(); i++) {
            System.out.print(collect.get(i) + " ");
        }
        System.out.println();
    }

    private static void mapTest(JavaRDD<Integer> javaRDD) {
        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer i) throws Exception {
                return i * 2;
            }
        });
        resultCollect(map);
    }
}
