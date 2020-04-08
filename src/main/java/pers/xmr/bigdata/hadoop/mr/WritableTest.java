package pers.xmr.bigdata.hadoop.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xmr
 * @date 2019/10/30 16:38
 * @description 自定义对象实现序列化传输
 */
public class WritableTest implements Writable {
    /*
        必须提供空参构造方法,因为反序列化时需要通过反射来调用空参的构造方法,没有提供会报错
     */
    public WritableTest() {

    }
    /*
        序列化方法 --> 将对象从内存持久化存储到硬盘
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }
    /*
        反序列化方法--> 讲硬盘中的持久化数据转换为内存中的对象
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
