import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.lang.StringBuilder;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LemmaLocation {

  private static HashMap<String,ArrayList<String>> lemmas = new HashMap<String,ArrayList<String>>();
  public static class LocationMapper
       extends Mapper<Object, Text, Text, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      try{
	      String[] split = value.toString().split("\t");
	      if(split.length != 2){
	        	split = value.toString().split("> ");
		        split[0] = split[0]+">";
	      }
	      Text location = new Text(split[0]);
	      split[1] = split[1].replaceAll("j","i");
        split[1] = split[1].replaceAll("\\p{Punct}","");
        split[1] = split[1].replaceAll("v","u");
	      StringTokenizer itr = new StringTokenizer(split[1]);
	      Text word = new Text();
	      while (itr.hasMoreTokens()) {
	        /*ArrayList<String> fetched = lemmas.get(temp);
	        if(fetched!=null){
	        	String[] lemmaArray = fetched.toArray(new String[fetched.size()]);
		        for(int i=0; i<lemmaArray.length; i++){
		            word.set(lemmaArray[i]);
		            context.write(word,location);
		        }
	        }else{
	        	word.set(temp);
		        context.write(word,location);
	        }*/
	        word.set(itr.nextToken().toLowerCase());
	        context.write(word,location);
	      }
	    }catch(ArrayIndexOutOfBoundsException e){
        //System.out.println(value.toString());
      }
    }
  }

  public static class LocationReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Text result = new Text();
      StringBuilder resString = new StringBuilder();
      for (Text location : values) {
        resString.append(location.toString() + ",");
      }
      String res = resString.toString();
      res = res.replaceAll(",$","");
      res = res.replaceAll(",$","");
      result.set(res);
      Text word = new Text();
      ArrayList<String> list = lemmas.get(key.toString());
      if(list==null){
      	list = new ArrayList<String>();
      	list.add(key.toString());	
      }     
      for(String x: list){
      	word.set(x);
      	context.write(word, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    //Reading the lemmas file and storing it in the hashmap.
    readLemmas();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "lemma location");
    job.setJarByClass(LemmaLocation.class);
    job.setMapperClass(LocationMapper.class);
    job.setCombinerClass(LocationReducer.class);
    job.setReducerClass(LocationReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(2);
    Path path = new Path(args[0]);
    int num_files = Integer.parseInt(args[2]);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(path);
    StringBuilder str = new StringBuilder();
    if(num_files > files.length){
      FileInputFormat.addInputPath(job, path);
    }else{
      for(int i=0; i<num_files;i++){
        str.append(files[i].getPath());
        if(i!=num_files-1)
          str.append(",");
      }
      FileInputFormat.addInputPaths(job, str.toString());
    }    
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static void readLemmas() throws IOException,FileNotFoundException{
    BufferedReader reader = new BufferedReader(new FileReader("new_lemmatizer.csv"));
    String line;
    while((line = reader.readLine())!=null){
      String[] split = line.split(",");
      ArrayList<String> list = new ArrayList<String>();
      for(int i=1; i<split.length; i++){
        list.add(split[i].toLowerCase());
      }
      lemmas.put(split[0].toLowerCase(),list);
    }
  }
}