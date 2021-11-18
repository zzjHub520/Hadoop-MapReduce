package cn.itcast.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 四个泛型解释：
 * keyin：k1的类型
 * valuein：v1的类型
 *
 * keyout：k2的类型
 * valueout：v2的类型
 *
 *
 */
public class WrdCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text=new Text();
        LongWritable longWritable = new LongWritable();

        String[] split = value.toString().split(",");

        for (String word : split) {
            text.set(word);
            longWritable.set(1);
            context.write(text, longWritable);
        }

    }
}


































