package util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

/**
 * Copyright (C)
 *
 * @program: kafkas
 * @description: hdfs的工具类
 * @author: 刘文强  kingcall
 * @create: 2018-05-21 10:16
 **/
public class HDFSUtil {

    public static Configuration conf=new Configuration();

    /**
     * 上传本地文件到hdfs集群    这个方法支持自动创建文件夹 最后一层就是文件的名字
     * @param fileName
     * @param hdfsPath
     * @throws IOException
     */
    public static void uploadFile(String fileName,String hdfsPath,String confoption) throws IOException{
        conf.set("fs.defaultFS", confoption);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem hdfs = FileSystem.get(conf);
        Path src = new Path(fileName);
        Path dst = new Path(hdfsPath);
        hdfs.copyFromLocalFile(src, dst);
        hdfs.close();
    }





}
