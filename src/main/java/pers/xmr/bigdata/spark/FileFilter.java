package pers.xmr.bigdata.spark;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 输入路径的文件过滤
 * 过滤掉临时文件和正在复制的文件
 * @author xmr
 *
 */
public class FileFilter implements PathFilter
{
    @Override
    public boolean accept(Path path) {

        String tmpStr = path.getName();
        if(tmpStr.contains(".tmp"))
        {
            return false;
        }
        else if(tmpStr.contains("_COPYING_"))
        {
            return false;
        }
        else
        {
            return true;
        }
    }
}
