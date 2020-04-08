package pers.xmr.bigdata.hadoop.mr;

import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @author xmr
 * @date 2019/10/30 16:54
 * @description 自定义InputFormat流程
 */
public class InputFormatTest implements InputFormat {
    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return null;
    }
}
