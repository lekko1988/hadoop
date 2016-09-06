package com.lekko.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.security.Timestamp;
import java.util.StringTokenizer;

/**
 * Created by root on 2016/9/2.
 */
public class MapReduceLekkoLogAnalysis {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Java的字符串分解类，默认分隔符“空格”、“制表符(‘\t’)”、“换行符(‘\n’)”、“回车符(‘\r’)”
            //若文件夹非空，则开始截取非空的地方加入测试文档中

                // 打印样本: Before Mapper:
                System.out.print("Before Mapper: " + key + ", " + value);

                String e = value.toString().trim();
                String uri= e.substring(e.indexOf(":/")+2);
//                //处理字符串 ，截取到最后的uri
//                String encoding = "utf-8";
//                //考虑到编码格式  建立读取流
//                InputStreamReader read = new InputStreamReader(new FileInputStream(e), encoding);
//                //读取数据流
//                BufferedReader bufferedReader = new BufferedReader(read);
                //对每行进行控制，然后进行处理
//                while ((lineTxt = bufferedReader.readLine()) != null) {
                    //word.set()Java数据类型与hadoop数据类型转换

                    //hadoop全局类context输出函数write;
                    context.write(new Text(uri), one);

                // 打印样本: After Mapper:/xxxx/xxxx, 1
                System.out.println("======" + "After Mapper:" + "uri" + ", " + one);
//                }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * 删除指定目录
     *
     * @param conf
     * @param dirPath
     * @throws IOException
     */
    private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
        if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has been deleted sucessfullly.");
            } else {
                System.out.println(targetPath + " deletion failed.");
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("lekko hadoop test start!");
        System.setProperty("HADOOP_USER_NAME", "root");



        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();


        conf.set("mapred.jar","D:\\hadoopTest\\out\\artifacts\\hadoopTest_jar\\hadoopTest.jar");


        conf.set("fs.default.name", "hdfs://master:9000");

        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapred.remote.os", "Linux");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        conf.set("yarn.resourcemanager.hostname", "master");
//        conf.set("yarn.resourcemanager.resource-tracker.address", "master:8031");
//        conf.set("yarn.resourcemanager.scheduler.address", "master:8030");

        job.setJarByClass(MapReduceRemoteControl.class);
        ((JobConf) job.getConfiguration()).setJar("D:\\hadoopTest\\out\\artifacts\\hadoopTest_jar\\hadoopTest.jar");

        job.setJobName("myjob4lekko_20150905_"+ System.currentTimeMillis());
        //一定要修改map reduce的执行路径！
        job.setMapperClass(MapReduceLekkoLogAnalysis.Map.class);
        //一定要修改map reduce的执行路径！
        job.setReducerClass(MapReduceLekkoLogAnalysis.Reduce.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs://master:9000/input/"));
        //先删除output目录
        deleteDir(conf, "hdfs://master:9000/output/");
        //设定输出目录
        TextOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output/"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        while (true) {
            if (job.waitForCompletion(true)) {
                System.out.println("lekko hadoop test deployment successfully!");
                break;
            }
        }
    }

}