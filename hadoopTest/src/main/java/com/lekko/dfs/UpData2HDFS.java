package com.lekko.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * Created by root on 2016/9/2.
 */
public class UpData2HDFS {
    static {
//        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) {
        //初始化输入流
        InputStream in = null;
        //初始化输出流
        OutputStream out = null;
        try {
            //上传文件,从windows上读取要传的文件
            in = new BufferedInputStream(new FileInputStream("D:\\hadoopTest\\input\\APIAccess.log.2016-09-05.241"));
            //在hadoop上新建文件
            FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000/input/APIAccess.log.2016-09-05.241"), new Configuration());
            //在hadoop上新建文件
            out = fs.create(new Path("hdfs://master:9000/input/APIAccess.log.2016-09-05.241"), new Progressable() {
                public void progress() {
                    System.out.println("********正在上传loading********");
                }
            });
            IOUtils.copyBytes(in, out, 4096, false);
            System.out.println("********上传结束finish********");
            //下载文件1
//            in = new URL("hdfs://hadoop1/input/fd.txt").openStream();
//            IOUtils.copyBytes(in, System.out, 4096, false);

            //下载文件2
//            FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1"), new Configuration());
//            in = fs.open(new Path("hdfs://hadoop1/input/fd.txt"));
//            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
