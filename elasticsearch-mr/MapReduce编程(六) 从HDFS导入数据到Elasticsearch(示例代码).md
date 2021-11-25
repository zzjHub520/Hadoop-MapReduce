# MapReduce编程(六) 从HDFS导入数据到Elasticsearch(示例代码)

[esc_ai](https://www.136.la/au/show-53266.html) 2020-09-07

栏目: [Web](https://www.136.la/tech/list-30216.html) ·

来源: [esc_ai](https://www.136.la/au/show-53266.html)

作者：esc_ai

**简介** 这篇文章主要介绍了MapReduce编程(六) 从HDFS导入数据到Elasticsearch(示例代码)以及相关的经验技巧，文章约19223字，浏览量358，点赞数1，值得参考！

# **一、Elasticsearch for Hadoop安装**

Elasticsearch for Hadoop并不像logstash、kibana一样是一个独立的软件，而是Hadoop和Elasticsearch交互所需要的jar包。所以，有直接下载和maven导入2种方式。安装之前确保JDK版本不要低于1.8，Elasticsearch版本不能低于1.0。
官网对声明是对Hadoop 1.1.x、1.2.x、2.2.x、2.4.x、2.6.x、2.7.x测试通过，支持较好，其它版本的也并不是不能用。

## **1.1 官网下载zip安装包**

官网给的Elasticsearch for Hadoop的地址https://www.elastic.co/downloads/hadoop，下载后解压后，把dist目录下jar包导入hadoop即可。

## **1.2 maven方式下载**

使用maven管理jar包更加方便，在pom.xml文件中加入以下依赖:

```
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop</artifactId>
  <version>2.3.3</version>
</dependency>
```

我之前使用Intellij Idea搭建了一个MapReduce的环境，参考这里[MapReduce编程(一) Intellij Idea配置MapReduce编程环境](http://blog.csdn.net/napoay/article/details/68491469)，pom.xml中再加入上面elasticsearch-hadoop的依赖即可，注意版本。

上面的依赖包含了MapReduce、Pig、Hive、Spark等完整的依赖，如果只想单独使用某一个功能，可以细化分别加入。

Map/Reduce:

```
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop-mr</artifactId>
  <version>2.3.3</version>
</dependency>
```

Apache Hive:

```
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop-hive</artifactId>
  <version>2.3.3</version>
</dependency>
```

Apache Pig.

```
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-hadoop-pig</artifactId>
  <version>2.3.3</version>
</dependency>
```

Apache Spark.

```
<dependency>
  <groupId>org.elasticsearch</groupId>
  <artifactId>elasticsearch-spark-20_2.10</artifactId>
  <version>2.3.3</version>
</dependency>
```

## **1.3 将ES-hadoop 的jar包加入环境变量**

把ES-HADOOP的jar包导入环境变量,编辑/etc/profile（或.bash_profile)，我的路径为：

```
/usr/local/Cellar/elasticsearch-hadoop-2.4.3
```

jar包位于elasticsearch-hadoop-2.4.3的dist目录下。编辑/etc/profile，加入一行：

```
#ES-HADOOP HOME
export EsHadoop_HOME=/usr/local/Cellar/elasticsearch-hadoop-2.4.3
export CLASSPATH=$CLASSPATH:$EsHadoop_HOME/dist/*
```

最后使profile文件生效:

```
source /etc/profile
```

# **二、准备数据**

准备一些测试数据，数据内容为json格式，每行是一条文档。把下列内容保存到blog.json中。

```
{"id":"1","title":"git简介","posttime":"2016-06-11","content":"svn与git的最主要区别..."}
{"id":"2","title":"ava中泛型的介绍与简单使用","posttime":"2016-06-12","content":"基本操作：CRUD ..."}
{"id":"3","title":"SQL基本操作","posttime":"2016-06-13","content":"svn与git的最主要区别..."}
{"id":"4","title":"Hibernate框架基础","posttime":"2016-06-14","content":"Hibernate框架基础..."}
{"id":"5","title":"Shell基本知识","posttime":"2016-06-15","content":"Shell是什么..."}
```

启动hadoop，上传blog.json到hdfs：

```
hadoop fs -put blog.json /work
```

# **三、从HDFS读取文档索引到ES**

从HDFS读取文档索引到Elasticsearch的代码:

```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;

/**
 * Created by bee on 4/1/17.
 */
public class HdfsToES {

    public static class MyMapper extends Mapper<Object, Text, NullWritable,
            BytesWritable> {

        public void map(Object key, Text value, Mapper<Object, Text,
                NullWritable, BytesWritable>.Context context) throws IOException, InterruptedException {
            byte[] line = value.toString().trim().getBytes();
            BytesWritable blog = new BytesWritable(line);
            context.write(NullWritable.get(), blog);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("es.nodes", "192.168.1.111:9200");
        conf.set("es.resource", "blog/csdn");
        conf.set("es.mapping.id", "id");
        conf.set("es.input.json", "yes");

        Job job = Job.getInstance(conf, "hadoop es write test");
        job.setMapperClass(HdfsToES.MyMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path
                ("hdfs://localhost:9000//work/blog.json"));
        job.waitForCompletion(true);
    }
}
```

倒入完成后会在Elasticsearch中生成blog索引。查看内容如下:
![这里写图片描述](https://img.136.la/20210502/af4fd2acb3a342378b395554e75ec7a6.jpg)

# **四、API分析**

Map过程，按行读入，input kye的类型为Object，input value的类型为Text。输出的key为NullWritable类型，NullWritable是Writable的一个特殊类，实现方法为空实现，不从数据流中读数据，也不写入数据，只充当占位符。MapReduce中如果不需要使用键或值，就可以将键或值声明为NullWritable，这里把输出的key设置NullWritable类型。输出为BytesWritable类型，把json字符串序列化。

因为只需要写入，没有Reduce过程。在main函数中，首先创Configuration()类的一个对象conf，通过conf配置一些参数。

- `conf.setBoolean("mapred.map.tasks.speculative.execution", false);`

  关闭mapper阶段的执行推测

- `conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);`

  关闭reducer阶段的执行推测

- `conf.set("es.nodes", "192.168.1.111:9200");`

  配置Elasticsearch的IP和端口

- `conf.set("es.resource", "blog/csdn");`

  设置索引到Elasticsearch的索引名和类型名。

- `conf.set("es.mapping.id", "id");`

  设置文档id，这个参数”id”是文档中的id字段

- `conf.set("es.input.json", "yes");`

  指定输入的文件类型为json。

- `job.setInputFormatClass(TextInputFormat.class);`

  设置输入流为文本类型

- `job.setOutputFormatClass(EsOutputFormat.class);`

  设置输出为EsOutputFormat类型。

- `job.setMapOutputKeyClass(NullWritable.class);`

  设置Map的输出key类型为NullWritable类型

- `job.setMapOutputValueClass(BytesWritable.class);`

  设置Map的输出value类型为BytesWritable类型

------

2017年8月21日更新：

5.4版本的map函数：

```
    public static class MyMapper extends Mapper<Object, Text, NullWritable,
            Text> {
        private Text line = new Text();
        public void map(Object key, Text value, Mapper<Object, Text,
                NullWritable, Text>.Context context) throws IOException, InterruptedException {

            if(value.getLength()>0){
                line.set(value);
                context.write(NullWritable.get(), line);
            }

        }
    }
```

# **五、参考资料**

https://www.elastic.co/guide/en/elasticsearch/hadoop/current/mapreduce.html