package com.lekko.mapreduce;

/**
 * Created by Administrator on 2016/9/1.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordCount {
    //继承泛型类Mapper
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
        //定义hadoop数据类型IntWritable实例one，并且赋值为1
        private final static IntWritable one = new IntWritable(1);
        //定义hadoop数据类型Text实例word
        private Text word = new Text();
        //实现map函数
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Java的字符串分解类，默认分隔符“空格”、“制表符(‘\t’)”、“换行符(‘\n’)”、“回车符(‘\r’)”
            int idx = value.toString().indexOf(" ");
            //若文件夹非空，则开始截取非空的地方加入测试文档中
            if (idx > 0) {
                String e = value.toString().substring(0, idx);
                //word.set()Java数据类型与hadoop数据类型转换
                word.set(e);
                //hadoop全局类context输出函数write;
                context.write(word, one);
            }
        }
    }

    //继承泛型类Reducer
    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        //实例化IntWritable
        private IntWritable result = new IntWritable();
        //实现reduce
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            //循环values，并记录单词个数
            for (IntWritable val : values) {
                sum += val.get();
            }
            //Java数据类型sum，转换为hadoop数据类型result
            result.set(sum);
            //输出结果到hdfs
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

    public static void main(String[] args) throws Exception {
        //实例化Configuration
        Configuration conf = new Configuration();
        //配置上传远程jar
        /*
        conf.set("mapred.jar", "hadoopstudy-1.0-SNAPSHOT.jar");
        conf.set("fs.default.name", "hdfs://master:9000");//namenode的地址
        conf.set("mapred.job.tracker", "master:9001");//namenode的地址</span>
        */
        /*
　　    GenericOptionsParser是hadoop框架中解析命令行参数的基本类。
　　    getRemainingArgs();返回数组【一组路径】
　　    */
        /*
　　    函数实现
　　    public String[] getRemainingArgs() {
        return (commandLine == null) ? new String[]{} : commandLine.getArgs();
　　    }*/
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //如果只有一个路径，则输出需要有输入路径和输出路径
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        //先删除output目录
        deleteDir(conf, otherArgs[otherArgs.length - 1]);

        //实例化job
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        /*
　　    指定CombinerClass类
　　    这里很多人对CombinerClass不理解
　　    */
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        //reduce输出Key的类型，是Text
        job.setOutputKeyClass(Text.class);
        // reduce输出Value的类型
        job.setOutputValueClass(IntWritable.class);
        //添加输入路径
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //添加输出路径
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        //提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}