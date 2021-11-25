# MapReduce编程(二) 排序(示例代码)

[esc_ai](https://www.136.la/au/show-53266.html) 2020-09-07

栏目: [Web](https://www.136.la/tech/list-30216.html) ·

来源: [esc_ai](https://www.136.la/au/show-53266.html)

作者：esc_ai

**简介** 这篇文章主要介绍了MapReduce编程(二) 排序(示例代码)以及相关的经验技巧，文章约12434字，浏览量436，点赞数4，值得参考！

# 一、问题描述

文件中存储了商品id和商品价格的信息，文件中每行2列，第一列文本类型代表商品id，第二列为double类型代表商品价格。数据格式如下:

```
pid0 334589.41
pid1 663306.49
pid2 499226.8
pid3 130618.22
pid4 513708.8
pid5 723470.7
pid6 998579.14
pid7 831682.84
pid8 87723.96
```

要求使用MapReduce，按商品的价格从低到高排序，输出格式仍为原来的格式：第一列为商品id，第二列为商品价格。

为了方便测试，写了一个DataProducer类随机产生数据。

```
package com.javacore.hadoop;

import java.io.*;
import java.util.Random;

/**
 * Created by bee on 3/25/17.
 */
public class DataProducer {
    public static void doubleProcuder() throws Exception {
        File f = new File("input/productDouble");
        if (f.exists()) {
            f.delete();
        }

        Random generator = new Random();
        double rangeMin = 1.0;
        double rangeMax = 999999.0;

        FileOutputStream fos = new FileOutputStream(f);
        OutputStreamWriter osq = new OutputStreamWriter(fos);
        BufferedWriter bfw = new BufferedWriter(osq);

        for (int i = 0; i < 100; i++) {
            double pValue = rangeMin + (rangeMax - rangeMin) * generator.nextDouble();
            pValue = (double) Math.round(pValue * 100) / 100;
            try {
                bfw.write("pid" + i + " " + pValue + "\n");

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        bfw.close();
        osq.close();
        fos.close();
        System.out.println("写入完成!");

    }


    public static void main(String[] args) throws Exception {
        doubleProcuder();
    }
}
```

# 二、MapReduce程序

```
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
 * Created by bee on 3/28/17.
 */
public class DataSortText {

    public static class Map extends Mapper<Object, Text, DoubleWritable, Text> {
        public static DoubleWritable pValue = new DoubleWritable();
        public static Text pId = new Text();

        //
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\s+");
            pValue.set(Double.parseDouble(line[1]));
            pId.set(new Text(line[0]));
            context.write(pValue, pId);
        }

    }

    public static class Reduce extends Reducer<DoubleWritable, Text,
            Text, DoubleWritable> {

        public void reduce(DoubleWritable key,Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            for (Text val:values){
                context.write(val,key);
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        FileUtil.deleteDir("output");
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        String[] otherargs=new
                String[]{"input/productDouble",
                "output"};

        if (otherargs.length!=2){
            System.err.println("Usage: mergesort <in> <out>");
            System.exit(2);
        }

        Job job=Job.getInstance();
        job.setJarByClass(DataSortText.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(otherargs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherargs[1]));
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }
}
```

# 三、输出

运行之后，输出结果如下。

```
pid8    87723.96
pid3    130618.22
pid9    171804.65
pid0    334589.41
pid10   468768.65
pid2    499226.8
pid4    513708.8
pid1    663306.49
pid5    723470.7
pid7    831682.84
pid6    998579.14
```

# 四、性能分析

为了测试MapReduce排序的性能，数据量分别用1万、10万、100万、1000万、1亿、5亿做测试，结果如下。

| 数量    | 文件大小 | 排序耗时 |
| :------ | :------- | :------- |
| 1万     | 177KB    | 6秒      |
| 10万    | 1.9MB    | 6秒      |
| 100 万  | 19.7MB   | 13秒     |
| 1000 万 | 206.8MB  | 60秒     |
| 1亿     | 2.17GB   | 9分钟    |
| 5亿     | 11.28GB  | 41分钟   |

------

附机器硬件配置:

```
内存:8 GB 1867 MHz DDR3
CPU:2.7 GHz Intel Core i5
磁盘:SSD
```



以上就是本文的全部内容，希望对大家的学习有所帮助，本文为博主原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接和本声明。 原文地址：http://blog.csdn.net/napoay/article/details/68922843