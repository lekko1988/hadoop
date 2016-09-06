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

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by root on 2016/9/2.
 */
public class MapReduceRemoteControl {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum = value.get();
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

        System.setProperty("HADOOP_USER_NAME", "root");
        System.out.println("lekko hadoop test start!");


        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();

        //先删除output目录
/*        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        deleteDir(conf, otherArgs[otherArgs.length - 1]);*/

        //conf.set("mapred.jar","D:\\hadoopTest\\out\\artifacts\\hadoopTest_jar\\hadoopTest.jar");



        conf.set("fs.default.name", "hdfs://master:9000");

        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapred.remote.os", "Linux");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        conf.set("yarn.resourcemanager.hostname", "master");
//        conf.set("yarn.resourcemanager.resource-tracker.address", "master:8031");
//        conf.set("yarn.resourcemanager.scheduler.address", "master:8030");

        job.setJarByClass(MapReduceRemoteControl.class);
        ((JobConf)job.getConfiguration()).setJar("D:\\hadoopTest\\out\\artifacts\\hadoopTest_jar\\hadoopTest.jar");

        job.setJobName("myjob4lekko_000000002");

        job.setMapperClass(MapReduceRemoteControl.Map.class);
        job.setReducerClass(MapReduceRemoteControl.Reduce.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs://master:9000/input/fd.txt"));
        TextOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output/fd11.txt"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        while (true) {
            if (job.waitForCompletion(true)) {
                break;
            }
        }
    }
}
