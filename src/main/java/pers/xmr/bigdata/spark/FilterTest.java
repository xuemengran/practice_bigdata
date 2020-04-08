package pers.xmr.bigdata.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author xmr
 * @date 2020/4/8 19:19
 * @description 测试spark过滤指定类型文件功能测试
 */
public class FilterTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spark.fileFilter");
        sparkConf.setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        Configuration conf  = jsc.hadoopConfiguration(); //通过spark上下文获取到hadoop的配置
        conf.set("fs.defaultFS", "hdfs://10.213.32.96:9000");
        conf.set("mapreduce.input.pathFilter.class", "pers.xmr.bigdata.spark.FileFilter"); // 设置过滤文件的类,这是关键类!!

        // 测试1: 采用会被过滤条件过滤的 .tmp文件进行测试,会抛出Input path does not exist异常
        // JavaRDD<String> testRdd = jsc.textFile("hdfs://10.213.32.96:9000/test/spark/part-00003.tmp");
        // testRdd.saveAsTextFile("hdfs://10.213.32.96:9000/test/spark/output1");

        // 测试2: 读取普通的文件,程序执行正常,按照给定路径输出
        // JavaRDD<String> testRdd2 = jsc.textFile("hdfs://10.213.32.96:9000/test/spark/part-00003");
        // testRdd2.saveAsTextFile("hdfs://10.213.32.96:9000/test/spark/output2");

        // 测试3: 读取路径,路径下面全部都是需要过滤的文件, 这里会报错 : java.io.IOException: Not a file: hdfs://10.213.32.96:9000/test/spark/output3
        // JavaRDD<String> testRdd3 = jsc.textFile("hdfs://10.213.32.96:9000/test/spark");
        // testRdd3.saveAsTextFile("hdfs://10.213.32.96:9000/test/spark/output3");

        // 测试4: 读取路径,路径下面全部都是需要过滤的文件,但是通过通配符匹配的形式
        // 这里没有报错,但是在output4下面只输出了一个_SUCCESS文件
        // JavaRDD<String> testRdd4 = jsc.textFile("hdfs://10.213.32.96:9000/test/spark/*");
        // testRdd4.saveAsTextFile("hdfs://10.213.32.96:9000/test/spark/output4");

        // 测试5: 读取路径,路径下面既有需要过滤的文件,也有正常文件 程序报错: java.io.IOException: Not a file: hdfs://10.213.32.96:9000/test/spark/output5
        // JavaRDD<String> testRdd5 = jsc.textFile("hdfs://10.213.32.96:9000/test/spark");
        // testRdd5.saveAsTextFile("hdfs://10.213.32.96:9000/test/spark/output5");

        // 测试6: 读取路径,路径下面既有需要过滤的文件,也有正常文件,但是通过通配符匹配的形式
        // 按照我们所期待的输出了相关文件
        JavaRDD<String> testRdd6 = jsc.textFile("hdfs://10.213.32.96:9000/test/spark/*");
        testRdd6.saveAsTextFile("hdfs://10.213.32.96:9000/test/spark/output6");
    }
}
