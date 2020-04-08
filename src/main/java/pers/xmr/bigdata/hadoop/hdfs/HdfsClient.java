package pers.xmr.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author xmr
 * @date 2019/12/11 18:34
 * @description Hdfs文件系统java api操作总结
 */
public class HdfsClient {

    private static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://linux01:9000");
        // conf.set("dfs.replication","2");
        return conf;
    }

    /**
     * 创建或删除操作
     *
     * @param conf Hdfs配置
     */
    private void mkdirsAndRemove(Configuration conf) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(new URI("hdfs://linux01:9000"), conf, "master")) {
            fileSystem.deleteOnExit(new Path("/20191210/xmr/test")); // 只删除最底层目录
            fileSystem.mkdirs(new Path("/20191210/xmr/test")); // 没有的话会递归创建
        }
    }

    /**
     * 文件拷贝操作--> 本地到hdfs, hdfs到本地
     *
     * @param conf HDFS配置
     */
    private void copyDir(Configuration conf) throws Exception {
        try (FileSystem fs = FileSystem.get(new URI("hdfs://linux01:9000"), conf, "master")) {
            fs.copyFromLocalFile(new Path("E:\\yinsheng\\data\\实时etl\\ogg问题.txt"), new Path("/20191211/etl"));
            // 配置优先级 : 代码 > classpath路径 > 服务器配置
            fs.copyToLocalFile(false, new Path("/20191211/etl/ogg问题.txt"), new Path(""));
        }
    }
    /*
        hsfs文件系统的其它操作
     */
    public void operate(Configuration conf) throws Exception {
        try(FileSystem fs = FileSystem.get(new URI("hdfs://linux01:9000"), conf, "master")){
            // 修改文件名称
            fs.rename(new Path("/20191211/etl/ogg问题.txt"), new Path("/20191211/etl/ogg问题_重命名.txt"));
            // 查看文件详情
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
            while (listFiles.hasNext()) {
                LocatedFileStatus status = listFiles.next();
                System.out.println(status.getPath().getName()); // 输出文件名称
                System.out.println(status.getLen()); //输出文件长度
                System.out.println(status.getPermission()); // 输出文件权限
                System.out.println(status.getGroup()); //输出文件分组
                // 获取存储的块信息
                BlockLocation[] blockLocations = status.getBlockLocations();
                for (BlockLocation blockLocation : blockLocations) {
                    // 获取块存储
                    String [] hosts = blockLocation.getHosts();
                    for (String host : hosts) {
                        System.out.println(host);
                    }
                }
            }
            // 文件和文件夹判断
            FileStatus[] listFileStatus = fs.listStatus(new Path("/"));
            for (FileStatus fileStatus: listFileStatus) {
                fileStatus.isFile(); // 是否是文件? 是的话返回true
                fileStatus.isDirectory(); // 是否是文件夹? 是的话返回true
            }

        }
    }

    /**
     * HDFS的IO操作
     * @param conf
     */
    private void hdfsIo(Configuration conf) throws Exception{
        try(FileSystem fs = FileSystem.get(new URI("hdfs://linux01:9000"), conf, "master")){
            // HDFS文件上传
            FileInputStream fis = new FileInputStream(new File("E:\\yinsheng\\data\\实时etl\\ogg问题.txt")); // 创建输入流
            FSDataOutputStream fos = fs.create(new Path("/20191219/test.txt")); // 获取输出流
            IOUtils.closeStream(fos);
            IOUtils.closeStream(fis); // 关闭资源

            // HDFS文件下载
            FSDataInputStream in = fs.open(new Path("/20191219/test.txt"));
            FileOutputStream out = new FileOutputStream(new  File("E:\\yinsheng\\data\\实时etl\\ogg问题.txt"));
            IOUtils.copyBytes(in, out, conf);
            IOUtils.closeStream(out);
            IOUtils.closeStream(in); // 关闭资源
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = HdfsClient.getConfiguration();
        HdfsClient hdfsClient = new HdfsClient();
        hdfsClient.mkdirsAndRemove(conf);
        hdfsClient.copyDir(conf);
    }
}
