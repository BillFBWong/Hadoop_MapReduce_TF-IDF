import java.io.IOException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFIDF {
    public static Integer FileCount = 0;

    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String pattern = "[^a-zA-Z0-9-]";

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取文件名
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            // 将每一行转化为一个 String
            String line = value.toString();
            // 将标点符号等字符用空格替换, 这样仅剩单词
            line = line.replaceAll(pattern, " ");
            // 将String划分为一个个的单词
            String[] words = line.split("\\s+");
            // 将每一个单词初始化为词频为1, 以(word,fileName)形式记录单词与文件名在 key 中
            // 与 Combiner 输出保持一致, 需要将词频转换为 Text 类型
            for (String word : words) {
                if (word.length() > 0) {
                    context.write(new Text(word + "," + fileSplit.getPath().getName()), new Text("1"));
                }
            }
        }
    }

    public static class TFIDFCombiner extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 初始化词频总数为0
            Integer count = 0;
            // 对 key 是同一文件中一样的单词, 执行词频汇总操作, 也就是同样的单词, 若是再出现则词频累加
            for (Text value : values) {
                    count += Integer.parseInt(value.toString());
            }
            // 确定分隔单词与文件名的位置
            Integer splitIndex = key.find(",");
            // 提取单词记录在 key, 提取文件名与词频连接以(fileName,count)形式记录在 value 中
            context.write(
                new Text(key.toString().substring(0, splitIndex)), 
                new Text(key.toString().substring(splitIndex+1) + "," + count));
        }
    }

    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 初始化单词所在文件的总数
            Integer count = 0;
            // 使用一个 List 来存储文件名-词频对
            List<String> fileList = new ArrayList<>();
            // 对 key 是一样的单词, 将统计的文件名和词频存入 List 中，该单词出现文件数加一
            for (Text value : values) {
                fileList.add("(" + value + ")");
                count += 1;
            }
            // 计算 IDF
            double idf = (double)Math.log((double)(FileCount)/count);
            // 最后输出汇总后的结果, 每个单词只会输出一次
            // 紧跟着该单词所在的文件与在文件中的词频, 以逗号分隔
            context.write(key, new Text(String.format("%.2f", idf) + "=" + String.join(", ", fileList)));
        }
    }

    //Main
    public static void main(String[] args) throws Exception {
        //统计文件数量
        Integer count = 0;
        File fd = new File(args[0]);
        File list[] = fd.listFiles();
        for(int i = 0; i < list.length; i++) {
            if(list[i].isFile()) {
            count++;
            }
        }
        FileCount = count;

        // 创建配置对象
        Configuration conf = new Configuration();
        // 创建Job对象
        Job job = Job.getInstance(conf, "TFIDF");
        // 设置运行Job的类
        job.setJarByClass(TFIDF.class);
        // 设置Mapper类
        job.setMapperClass(TFIDFMapper.class);
        // 设置Combiner类
        job.setCombinerClass(TFIDFCombiner.class);
        // 设置Reducer类
        job.setReducerClass(TFIDFReducer.class);
        // 设置Map输出的Key value
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置Reduce输出的Key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交job
        boolean b = job.waitForCompletion(true);
        if(!b) {
            System.out.println("TF-IDF task fail!");
        }
    }
}
