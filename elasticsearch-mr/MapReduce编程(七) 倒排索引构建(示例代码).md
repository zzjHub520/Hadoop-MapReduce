# MapReduce编程(七) 倒排索引构建(示例代码)

[esc_ai](https://www.136.la/au/show-53266.html) 2020-09-08

栏目: [Web](https://www.136.la/tech/list-30216.html) ·

来源: [esc_ai](https://www.136.la/au/show-53266.html)

作者：esc_ai

**简介** 这篇文章主要介绍了MapReduce编程(七) 倒排索引构建(示例代码)以及相关的经验技巧，文章约18737字，浏览量303，点赞数3，值得参考！

# **一、倒排索引简介**

> 倒排索引（英语：Inverted index），也常被称为反向索引、置入档案或反向档案，是一种索引方法，被用来存储在全文搜索下某个单词在一个文档或者一组文档中的存储位置的映射。它是文档检索系统中最常用的数据结构。

以英文为例，下面是要被索引的文本：

```
T0="it is what it is"
T1＝"what is it"
T2＝"it is a banana"
```

我们就能得到下面的反向文件索引：

```
 "a":      {2}
 "banana": {2}
 "is":     {0, 1, 2}
 "it":     {0, 1, 2}
 "what":   {0, 1}
```

检索的条件”what”, “is” 和 “it” 将对应这个集合：`{0, 1}&{0, 1, 2}& {0, 1, 2}={0,1}`

对于中文分词，可以使用开源的中文分词工具，这里使用ik-analyzer。

准备几个文本文件，写入内容做测试。

file1.txt内容如下:

```
事实上我们发现，互联网裁员潮频现甚至要高于其他行业领域
```

file2.txt内容如下:

```
面对寒冬，互联网企业不得不调整人员结构，优化雇员的投入产出
```

file3.txt内容如下:

```
在互联网内部，由于内部竞争机制以及要与竞争对手拼进度
```

file4.txt内容如下:

```
互联网大公司职员虽然可以从复杂性和专业分工中受益
互联网企业不得不调整人员结构
```

# **二、添加依赖**

出了hadoop基本的jar包意外，加入中文分词的`lucene-analyzers-common`和`ik-analyzers`：

```
   <!--Lucene分词模块-->
    <dependency>
      <groupId>org.apache.lucene</groupId>
      <artifactId>lucene-analyzers-common</artifactId>
      <version>6.0.0</version>
    </dependency>

 <!--IK分词 -->
    <dependency>
      <groupId>cn.bestwu</groupId>
      <artifactId>ik-analyzers</artifactId>
      <version>5.1.0</version>
    </dependency>
```

# **三、MapReduce程序**

关于Lucene 6.0中IK分词的配置参考http://blog.csdn.net/napoay/article/details/51911875，MapReduce程序如下。

```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bee on 4/4/17.
 */
public class InvertIndexIk {

    public static class InvertMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName()
                    .toString();
            Text fname = new Text(filename);
            IKAnalyzer6x analyzer = new IKAnalyzer6x(true);
            String line = value.toString();
            StringReader reader = new StringReader(line);
            TokenStream tokenStream = analyzer.tokenStream(line, reader);
            tokenStream.reset();
            CharTermAttribute termAttribute = tokenStream.getAttribute
                    (CharTermAttribute.class);
            while (tokenStream.incrementToken()) {
                Text word = new Text(termAttribute.toString());
                context.write(word, fname);
            }
        }

    }


    public static class InvertReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,Reducer<Text,Text,
                Text,Text>.Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text val : values) {
                if (map.containsKey(val.toString())) {

                    map.put(val.toString(),map.get(val.toString())+1);

                } else {
                    map.put(val.toString(),1);
                }

            }
            int termFreq=0;
            for (String mapKey:map.keySet()){
                termFreq+=map.get(mapKey);
            }
            context.write(key,new Text(map.toString()+"  "+termFreq));
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        HadoopUtil.deleteDir("output");
        Configuration conf=new Configuration();

        String[] otherargs=new
                String[]{"input/InvertIndex",
                "output"};

        if (otherargs.length!=2){
            System.err.println("Usage: mergesort <in> <out>");
            System.exit(2);
        }

        Job job=Job.getInstance();
        job.setJarByClass(InvertIndexIk.class);
        job.setMapperClass(InvertIndexIk.InvertMapper.class);
        job.setReducerClass(InvertIndexIk.InvertReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(otherargs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherargs[1]));
        System.exit(job.waitForCompletion(true) ? 0: 1);

    }
}
```

# **四、运行结果**

输出如下:

```
专业分工    {file4.txt=1}  1
中   {file4.txt=1}  1
事实上 {file1.txt=1}  1
互联网 {file1.txt=1, file3.txt=1, file4.txt=2, file2.txt=1}  5
人员  {file4.txt=1, file2.txt=1}  2
企业  {file4.txt=1, file2.txt=1}  2
优化  {file2.txt=1}  1
内部  {file3.txt=2}  2
发现  {file1.txt=1}  1
受益  {file4.txt=1}  1
复杂性 {file4.txt=1}  1
大公司 {file4.txt=1}  1
寒冬  {file2.txt=1}  1
投入产出    {file2.txt=1}  1
拼   {file3.txt=1}  1
潮   {file1.txt=1}  1
现   {file1.txt=1}  1
竞争对手    {file3.txt=1}  1
竞争机制    {file3.txt=1}  1
结构  {file4.txt=1, file2.txt=1}  2
职员  {file4.txt=1}  1
行业  {file1.txt=1}  1
裁员  {file1.txt=1}  1
要与  {file3.txt=1}  1
调整  {file4.txt=1, file2.txt=1}  2
进度  {file3.txt=1}  1
雇员  {file2.txt=1}  1
面对  {file2.txt=1}  1
领域  {file1.txt=1}  1
频   {file1.txt=1}  1
高于  {file1.txt=1}  1
```

结果有三列，依次为词项、词项在单个文件中的词频以及总的词频。

# **五、参考资料**

1.[https://zh.wikipedia.org/wiki/ 倒排索引](https://zh.wikipedia.org/wiki/倒排索引)
\2. [Lucene 6.0下使用IK分词器](http://blog.csdn.net/napoay/article/details/51911875)



以上就是本文的全部内容，希望对大家的学习有所帮助，本文为博主原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接和本声明。 原文地址：http://blog.csdn.net/napoay/article/details/69069846