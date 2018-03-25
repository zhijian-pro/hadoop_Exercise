package coom.anthony.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description: 使用MapReduce开发Wordcount程序
 * @Date: Created in 10:36 2018/3/25
 * @Author: Anthony_Duan
 */
public class WordCountApp {

    /**
     * map:读取输入的文件
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        LongWritable one  = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//            接受到的每一行数据
            String line = value.toString();

//            按照指定分隔符进行拆分
            String words[] = line.split(" ");

            for (String word: words) {
//                通过上下文把map结果输出
                context.write(new Text(word),one);
            }
        }
    }


    /**
     * reducer:归并操作
     */
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
           long sum = 0;
            for (LongWritable value:values) {
                sum += value.get();
            }
//          最终结果输出
            context.write(key,new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        创建Configuration
        Configuration configuration = new Configuration();

//        创建job   抛出异常快捷键 alt+enter
        Job job = Job.getInstance(configuration,"wordcount");

//        设置job的处理类
        job.setJarByClass(WordCountApp.class);
//        设置作业的输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

//         设置map相关参数
        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


//        设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        设置作业的输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1 );


    }
}
