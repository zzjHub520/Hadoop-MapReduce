# [Ryan_Y](https://www.cnblogs.com/kaisne/)

热爱技术、热爱生活。

## [hadoop 读写 elasticsearch 初探](https://www.cnblogs.com/kaisne/p/3930677.html)

1、参考文档:

http://www.elasticsearch.org/guide/en/elasticsearch/hadoop/current/configuration.html

http://www.elasticsearch.org/guide/en/elasticsearch/hadoop/current/mapreduce.html#_emphasis_old_emphasis_literal_org_apache_hadoop_mapred_literal_api

 

2、Mapreduce相关配置

 

 

//以下ES配置主要是提供给ES的Format类进行读取使用

Configuration conf = **new** Configuration();

conf.set(ConfigurationOptions.ES_NODES, "127.0.0.1");

conf.set(ConfigurationOptions.ES_PORT, "9200");

conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

//设置读取和写入的资源index/type

conf.set(ConfigurationOptions.ES_RESOURCE, "helloes/demo"); //read Target index/type

 

 

 

//假如只是想检索部分数据，可以配置ES_QUERY

//conf.set(ConfigurationOptions.ES_QUERY, "?q=me*");

 

//配置Elasticsearch为hadoop开发的format等

Job job = Job.*getInstance*(conf,ElasticsearchIndexMapper.**class**.getSimpleName());

job.setJarByClass(ElasticsearchIndexBuilder.**class**);

job.setSpeculativeExecution(**false**);//Disable speculative execution

job.setInputFormatClass(EsInputFormat.**class**);  

 

//假如数据输出到HDFS，指定Map的输出Value的格式。并且选择Text格式

job.setOutputFormatClass(TextOutputFormat.**class**);

job.setMapOutputValueClass(Text.**class**);

job.setMapOutputKeyClass(NullWritable.**class**);  

 

 

//如果选择输入到ES

job.setOutputFormatClass(EsOutputFormat.**class**);//输出到

job.setMapOutputValueClass(LinkedMapWritable.**class**);//输出的数值类 

job.setMapOutputKeyClass(Text.**class**);  //输出的Key值类

 

 

job.setMapperClass(ElasticsearchIndexMapper.**class**);

FileInputFormat.*addInputPath*(job, **new** Path("hdfs://localhost:9000/es_input"));

FileOutputFormat.*setOutputPath*(job, **new** Path("hdfs://localhost:9000/es_output"));

job.setNumReduceTasks(0);

job.waitForCompletion(**true**);

 

3、对应的Mapper类ElasticsearchIndexMapper

**public** **class** ElasticsearchIndexMapper **extends** Mapper {

@Override

**protected** **void** **map**(Object key, Object value, Context context)

​    **throws** IOException, InterruptedException {

//假如我这边只是想导出数据到HDFS

 

 LinkedMapWritable doc = (LinkedMapWritable) value;  

 Text docVal = **new** Text();

  docVal.set(doc.toString());

 context.write(NullWritable.get(), docVal);

}

}

4、小结

hadoop-ES读写最主要的就是ESInputFormat、ESOutputFormat的参数配置（Configuration）。

另外 其它数据源操作（Mysql等）也是类似，找到对应的InputFormat，OutputFormat配置上环境参数。