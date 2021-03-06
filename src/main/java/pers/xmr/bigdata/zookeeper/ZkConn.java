package pers.xmr.bigdata.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;

/**
 * @author xmr
 * @date 2019/8/23 14:43
 * @description zookeeper原生api使用简介
 */
public class ZkConn {
    /*
        1. 创建连接
        2. 查看子节点
        3. 创建目录节点
        4. 修改节点数据
        5. 删除节点
        6. 关闭连接
     */
    public static void main(String[] args)
            throws IOException, KeeperException, InterruptedException {
        Thread.sleep(1000);
        String splitStr = "******************************************************************************************";
        String rootPath = "/";
        String parentNode = "/node";
        String childNode = "/node/sub1";
        /*
         *  创建一个与服务器的连接
         *  参数一：服务器地址和端口号（该端口号值服务器允许客户端连接的端口号）
         *  参数二：连接会话超时时间
         *  参数三：观察者，连接成功会触发该观察者。不过只会触发一次。
         *      该Watcher会获取各种事件的通知
         */

        try( ZooKeeper zk = new ZooKeeper("linux01:2181", 60000, new Watcher() {
            // 监控所有被触发的事件
            public void process(WatchedEvent event) {
                System.out.println("监控所有被触发的事件:EVENT:" + event.getType());
            }
        })){
            System.out.println(splitStr);
            // 查看根节点的子节点
            System.out.println("查看根节点的子节点:ls / => " + zk.getChildren(rootPath, true));
            // 查看根节点的子节点数据
            for (String childData :  zk.getChildren(rootPath, true)) {
                System.out.println("根节点的子节点为: " + childData);
                // 节点不存在时报错 :  KeeperException$NoNodeException: KeeperErrorCode = NoNode for /sdf
                // 节点存在,但内容为空时,报错 : java.lang.NullPointerException
                // 节点名称格式有误(不是 / + 节点名)形式,报错 : Path must start with / character
                // System.out.println("该节点的数据为: " + new String(zk.getData(rootPath + "notexists", false, null)));
                // System.out.println("该节点的数据为: " + new String(zk.getData(rootPath + childData, false, null)));
                // System.out.println("该节点的数据为: " + new String(zk.getData(childData, false, null)));
            }

            System.out.println(splitStr);
            // 创建一个目录节点
            if (zk.exists(parentNode, true) == null) {
                /*
                 * 参数一：路径地址
                 * 参数二：想要保存的数据，需要转换成字节数组
                 * 参数三：ACL访问控制列表（Access control list）,
                 *      参数类型为ArrayList<ACL>，Ids接口提供了一些默认的值可以调用。
                 *      OPEN_ACL_UNSAFE     This is a completely open ACL
                 *                          这是一个完全开放的ACL，不安全
                 *      CREATOR_ALL_ACL     This ACL gives the
                 *                           creators authentication id's all permissions.
                 *                          这个ACL赋予那些授权了的用户具备权限
                 *      READ_ACL_UNSAFE     This ACL gives the world the ability to read.
                 *                          这个ACL赋予用户读的权限，也就是获取数据之类的权限。
                 * 参数四：创建的节点类型。枚举值CreateMode
                 *      PERSISTENT (0, false, false)
                 *      PERSISTENT_SEQUENTIAL (2, false, true)
                 *          这两个类型创建的都是持久型类型节点，回话结束之后不会自动删除。
                 *          区别在于，第二个类型所创建的节点名后会有一个单调递增的数值
                 *      EPHEMERAL (1, true, false)
                 *      EPHEMERAL_SEQUENTIAL (3, true, true)
                 *          这两个类型所创建的是临时型类型节点，在回话结束之后，自动删除。
                 *          区别在于，第二个类型所创建的临时型节点名后面会有一个单调递增的数值。
                 * 最后create()方法的返回值是创建的节点的实际路径
                 */
                zk.create(parentNode, "conan".getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("创建一个目录节点:create /node conan");
                /*
                 *  查看/node节点数据，这里应该输出"conan"
                 *  参数一：获取节点的路径
                 *  参数二：说明是否需要观察该节点，设置为true，则设定共享默认的观察器
                 *  参数三：stat类，保存节点的信息。例如数据版本信息，创建时间，修改时间等信息
                 */
                System.out.println("查看/node节点数据:get /node => "
                        + new String(zk.getData(parentNode, false, null)));
                /*
                 * 查看根节点
                 * 在此查看根节点的值，这里应该输出上面所创建的/node节点
                 */
                System.out.println("查看根节点:ls / => " + zk.getChildren("/", true));
            }
            System.out.println(splitStr);
            // 创建一个子目录节点
            if (zk.exists(childNode, true) == null) {
                zk.create(childNode, "sub1".getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("创建一个子目录节点:create /node/sub1 sub1");
                // 查看node节点
                System.out.println("查看node节点:ls /node => "
                        + zk.getChildren(parentNode, true));
            }
            System.out.println(splitStr);
            /*
             *  修改节点数据
             *  修改的数据会覆盖上次所设置的数据
             *  setData()方法参数一、参数二不多说，与上面类似。
             *  参数三：数值型。需要传入该界面的数值类型版本号！！！
             *      该信息可以通过Stat类获取，也可以通过命令行获取。
             *      如果该值设置为-1，就是忽视版本匹配，直接设置节点保存的值。
             */
            if (zk.exists(parentNode, true) != null) {
                zk.setData(parentNode, "changed".getBytes(), -1);
                // 查看/node节点数据
                System.out.println("修改节点数据:get /node => "
                        + new String(zk.getData(parentNode, false, null)));
            }
            System.out.println(splitStr);
            // 删除节点
            if (zk.exists(childNode, true) != null) {
                zk.delete(childNode, -1);
                zk.delete(parentNode, -1);
                // 查看根节点
                System.out.println("删除节点:ls / => " + zk.getChildren("/", true));
            }
            // 关闭连接, 这里利用了jdk1.7 try,catch自动释放资源的特性,无需手动关闭连接
            // zk.close();
        }

    }
}
