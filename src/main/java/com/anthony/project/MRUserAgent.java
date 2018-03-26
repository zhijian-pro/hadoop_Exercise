package com.anthony.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description: 利用MapReduce解析日志UA
 * @Date: Created in 16:28 2018/3/26
 * @Author: Anthony_Duan
 */
public class MRUserAgent {

    /**
     * map处理类
     * <LongWritable, Text, Text, LongWritable> 前两个是输入类型 后两个是输出类型
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        LongWritable one = new LongWritable(1);

        //在外面声明是为了cleanup可以对其进行释放的操作
        UserAgentParser userAgentParser;

        //setup只执行一遍，可以减少资源开销，不必每次都创建一个解析类 Called once at the beginning of the task.
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser = new UserAgentParser();
        }

        //Called once at the end of the task.
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String source = line.substring(getCharacterPosition(line, "\"", 7) + 1);
            UserAgent agent = userAgentParser.parse(source);
            String browser = agent.getBrowser();

            //通过上下文把map结果输出
            context.write(new Text(browser), one);
        }
    }


    /**
     * 获取指定字符串中指定字符串出现的索引位置
     *
     * @param value    源日志文件
     * @param operator 指定的字符串
     * @param index    第几个指定的字符串
     * @return 返回指定字符串出现的索引的位置
     */
    private static int getCharacterPosition(String value, String operator, int index) {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdex = 0;
        while (slashMatcher.find()) {
            mIdex++;
            if (mIdex == index) {
                break;
            }
        }
        return slashMatcher.start();
    }

    /**
     * 分区类
     * 这里用switch key为String类型 需要将编译环境提升为1.7以上
     */
    public static class MyPartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
            String key = text.toString();

            switch (key) {
                case "MSIE":
                    return 0;
                case "Safari":
                    return 1;
                case "Chrome":
                    return 2;
                case "Firefox":
                    return 3;
            }
            return 4;
        }
    }


    /**
     * reduce类  参数<Text, LongWritable, Text, LongWritable> 前两个是输入类型 后两个是输出类型
     */
    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * 设置驱动类
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        //创建配置信息
        Configuration configuration = new Configuration();

        //命令行获取参数
        Path outputPath = new Path(args[1]);

        //获取配置参数
        FileSystem fileSystem = FileSystem.get(configuration);

        //如果输出目录存在删除目录
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("outputPath is exists,but is has delete");
        }

        //获取job 实例  传入配置和job 名字
        Job job = Job.getInstance(configuration, "MRUserAgent");


        //设置job处理类
        job.setJarByClass(MRUserAgent.class);

        //设置map处理类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //配置合并功能
        job.setCombinerClass(MyReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //配置分区
        job.setPartitionerClass(MRUserAgent.MyPartitioner.class);
        //设置分区个数 5个reduce 每一个分区一个
        job.setNumReduceTasks(5);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
