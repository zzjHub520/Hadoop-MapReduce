# MapReduce编程(四) 求均值(示例代码)

[esc_ai](https://www.136.la/au/show-53266.html) 2020-09-07

栏目: [Web](https://www.136.la/tech/list-30216.html) ·

来源: [esc_ai](https://www.136.la/au/show-53266.html)

作者：esc_ai

**简介** 这篇文章主要介绍了MapReduce编程(四) 求均值(示例代码)以及相关的经验技巧，文章约10436字，浏览量200，点赞数4，值得参考！

# 一、问题描述

> 三个文件中分别存储了学生的语文、数学和英语成绩，输出每个学生的平均分。

数据格式如下：
Chinese.txt

```
张三    78
李四    89
王五    96
赵六    67
```

Math.txt

```
张三    88
李四    99
王五    66
赵六    77
```

English.txt

```
张三    80
李四    82
王五    84
赵六    86
```

# 二、MapReduce[**编程**](https://www.136.la/)

```java
package com.javacore.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Created by bee on 3/29/17.
 */
public class StudentAvgDouble {

    public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           String eachline = value.toString();
           StringTokenizer tokenizer = new StringTokenizer(eachline, "\n");
            while (tokenizer.hasMoreElements()) {
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizer
                        .nextToken());
                String strName = tokenizerLine.nextToken();
                String strScore = tokenizerLine.nextToken();
                Text name = new Text(strName);
                IntWritable score = new IntWritable(Integer.parseInt(strScore));
                context.write(name, score);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context
                context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            DoubleWritable avgScore = new DoubleWritable(sum / count);
            context.write(key, avgScore);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //删除output文件夹
        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();
        String[] otherArgs = new String[]{"input/studentAvg", "output"};
        if (otherArgs.length != 2) {
            System.out.println("参数错误");
            System.exit(2);
        }

        Job job = Job.getInstance();
        job.setJarByClass(StudentAvgDouble.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
```

# 三、StringTokenizer和Split的用法对比

map函数里按行读入，每行按空格切开，之前我采用的split()函数切分，代码如下。

```java
 String eachline = value.toString();
 for (String eachline : lines) {
                System.out.println("eachline:\t"+eachline);
                String[] words = eachline.split("\\s+");
                Text name = new Text(words[0]);
                IntWritable score = new IntWritable(Integer.parseInt(words[1]));
                context.write(name, score);
            }
```

这种方式简单明了，但是也存在缺陷，对于非正常编码的空格有时候会出现切割失败的情况。
StringTokenizer是java.util包中分割解析类，StringTokenizer类的构造函数有三个:

1. `StringTokenizer（String str）`：java默认的分隔符是“空格”、“制表符（‘\t’）”、“换行符(‘\n’）”、“回车符（‘\r’）。

2. `StringTokenizer（String str,String delim）`:可以构造一个用来解析str的StringTokenizer对象，并提供一个指定的分隔符。

3. `StringTokenizer（String str,String delim,boolean returnDelims）`：构造一个用来解析str的StringTokenizer对象，并提供一个指定的分隔符，同时，指定是否返回分隔符。

   StringTokenizer和Split都可以对字符串进行切分，StringTokenizer的性能更高一些，分隔符如果用到一些特殊字符，StringTokenizer的处理结果更好。

# 四、运行结果

```
张三  82.0
李四  90.0
王五  82.0
赵六  76.66666666666667
```



以上就是本文的全部内容，希望对大家的学习有所帮助，本文为博主原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接和本声明。 原文地址：http://blog.csdn.net/napoay/article/details/68923805