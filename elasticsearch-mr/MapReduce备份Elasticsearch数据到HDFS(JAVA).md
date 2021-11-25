# [【原创】MapReduce备份Elasticsearch数据到HDFS(JAVA)](https://www.cnblogs.com/abcdefghijklmnopqrstuvwxyz/p/7828325.html)

```
一、环境：JAVA8，Elasticsearch-5.6.2，Hadoop-2.8.1
二、实现功能：mapreduce读elasticsearch数据、输出parquet文件、多输出路径
三、主要依赖
```

[![复制代码](https://common.cnblogs.com/images/copycode.gif)](javascript:void(0);)

```
<dependency>
    <groupId>org.elasticsearch.client</groupId>
    <artifactId>transport</artifactId>
    <version></version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-to-slf4j</artifactId>
    <version></version>
</dependency>
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version></version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version></version>
</dependency>
<dependency>
    <groupId>org.apache.parquet</groupId>
    <artifactId>parquet-avro</artifactId>
    <version></version>
</dependency>
<dependency>
    <groupId>org.elasticsearch</groupId>
    <artifactId>elasticsearch-hadoop-mr</artifactId>
    <version></version>
</dependency>
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-mapred</artifactId>
    <version></version>
</dependency>
```

[![复制代码](https://common.cnblogs.com/images/copycode.gif)](javascript:void(0);)

 

```
四、主要代码
1.public class Job extends Configured implements Tool
```

[![复制代码](https://common.cnblogs.com/images/copycode.gif)](javascript:void(0);)

```
 1 Configuration conf = getConf();
 2 conf.set(ConfigurationOptions.ES_NODES,"127.0.0.1");
 3 conf.set(ConfigurationOptions.ES_PORT,"9200");
 4 conf.set(ConfigurationOptions.ES_RESOURCE, "index/type");
 5 conf.set(ConfigurationOptions.ES_QUERY, "?q=*");
 6 Job job = Job.getInstance(conf);
 7 // ...（其他不重要的设置）
 8 // set input
 9 job.setInputFormatClass(EsInputFormat.class);
10 // set output
11 job.setOutputFormatClass(AvroParquetOutputFormat.class);
12 AvroParquetOutputFormat.setOutputPath(job, ${outputDir});
13 AvroParquetOutputFormat.setSchema(job, ${schema});
14 AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
15 AvroParquetOutputFormat.setCompressOutput(job, true);
16 AvroParquetOutputFormat.setBlockSize(job, ${size});
17 for(String name: ${list}){
18 　　MultipleOutputs.addNamedOutput(job, name, AvroParquetOutputFormat.class, Void.class, GenericRecord.class);
19 }
```

[![复制代码](https://common.cnblogs.com/images/copycode.gif)](javascript:void(0);)

2.public class Mapper extends Mapper<Text, MapWritable, ${KeyType}, ${ValueType}>

// 代码一般，略

3.public class Reducer extends Reducer<${KeyType}, ${ValueType}, Void, GenericRecord>

[![复制代码](https://common.cnblogs.com/images/copycode.gif)](javascript:void(0);)

```
 1 private MultipleOutputs<Void, GenericRecord> multipleOutputs;
 2 
 3 @Override
 4 protected void setup(Context context) throws IOException, InterruptedException {
 5     multipleOutputs = new MultipleOutputs<>(context);
 6 }
 7 
 8 @Override
 9 public void reduce(${KeyType} key, Iterable<${ValueType}> values, Context context) throws IOException, InterruptedException {
10     for(${ValueType} value:values){
11     　　GenericData.Record avroRecord = new GenericData.Record(ReflectData.get().getSchema(${实体类}.class));// value转实体类
12         avroRecord.put(${字段名}, ${字段值});
13     　　// ... n多字段   　　　　　　　　
14         multipleOutputs.write(${Job中的name}, null, avroRecord, ${输出hdfs的绝对路径});
15     }
16 }
17 
18 @Override
19 protected void cleanup(Context context) throws IOException, InterruptedException {
20     multipleOutputs.close();
21 }
```

[![复制代码](https://common.cnblogs.com/images/copycode.gif)](javascript:void(0);)

```
五、遇到的问题
1.查询字符串scroll失败
ConfigurationOptions.ES_QUERY，不需要urlEncode，否则反而会解析失败
例如查询带时间范围：?q=event_time:>=1509465600 AND event_time:<1512057600

2.多输出路径重复跑job，根路径冲突
Job中的输出路径不能存在否则会抛异常“org.apache.hadoop.mapred.FileAlreadyExistsException”，所以在创建Job时需要判断输出路径是否存在，存在则删除。
当时用MultipleOutputs时，Job中的${outputDir}和Reducer中的${输出hdfs的绝对路径}可以是完全不同的目录，Job中的输出路径会保存_matadata等不是很重要的数据（parquet本身包含这些信息），Reducer中的输出路径为想要的输出路径，路径下只保存parquet文件。
重复执行相同的Job时删除Job中的输出路径，主要数据没有影响，另外如果Reducer的输出路径有冲突可以在Job中循环删除。
```