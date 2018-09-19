package util;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Copyright (C)
 *
 * @program: kafkas
 * @description: zookeeper的util
 * @author: 刘文强  kingcall
 * @create: 2018-04-24 18:45
 **/
public class ZKUtil {
    static String connectionString = "master:2181,slave1:2181,slave2:2181";
    static int sessionTimeout = 30000;
    static ZooKeeper zk;
    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        createNode("/kingcall","kingcall");
    }

    /**
     * 创建节点
     * @param path 节点路径
     * @param data 节点数据
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void createNode(String path, String data) throws KeeperException, InterruptedException, IOException {
        zk = new ZooKeeper(connectionString,sessionTimeout,null);
        zk.create(path,data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

}
