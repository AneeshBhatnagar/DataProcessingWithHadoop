import java.io.IOException;
import java.lang.StringBuilder;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeCooccurence {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, CustomMapWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = value.toString().split(" ");
      int n = split.length;
      for(int i=0; i<n; i++){
        CustomMapWritable map = new CustomMapWritable();
        word.set(split[i]);
        for(int j=i+1; j<n; j++){
          Text text = new Text(split[j]);
          if(map.containsKey(text)){
            IntWritable val = (IntWritable)map.get(text);
            map.remove(text);
            val.set(val.get() + 1);
            map.put(text,val);
          }else{
            map.put(text,one);
          }
        }
        context.write(word,map);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,CustomMapWritable,Text,CustomMapWritable> {

    public void reduce(Text key, Iterable<CustomMapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      CustomMapWritable result = new CustomMapWritable();
      for (CustomMapWritable map : values) {
         Set<Writable> mapKeys = map.keySet();
         for (Writable mapKey : mapKeys){
          IntWritable newVal = (IntWritable)map.get(mapKey);
          if(result.containsKey(mapKey)){
            IntWritable val = (IntWritable)result.get(mapKey);
            result.remove(mapKey);
            result.put(mapKey, new IntWritable(val.get() + newVal.get()));
          }else{
            result.put(mapKey,newVal);
          }
         }
      }
      context.write(key, result);
    }
  }

  private static class CustomMapWritable extends MapWritable{
    @Override
    public String toString(){
      StringBuilder result = new StringBuilder();
      Set<Writable> keys = this.keySet();
      int size = keys.size();
      int i = 0;
      result.append("{");
      for(Writable key: keys){
        i++;
        result.append(key.toString() + ":" + this.get(key));
        if(i!=size){
          result.append("; ");
        }
      }
      result.append("}");
      return result.toString();
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "stripe cooccurence");
    job.setJarByClass(StripeCooccurence.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CustomMapWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CustomMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}