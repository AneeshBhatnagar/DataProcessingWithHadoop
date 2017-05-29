import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.StringTokenizer;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.lang.Integer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Word3grams {

  private static HashMap<String,ArrayList<String>> lemmas = new HashMap<String,ArrayList<String>>();

  public static class PairMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      try{
	      String[] split = value.toString().split("\t");
	      String[] strArray;
	      if(split.length != 2){
	        split = value.toString().split("> ");
	        split[0] = split[0]+">";
	      }
	      Text location = new Text(split[0]);
	      split[1] = split[1].replaceAll("\\p{Punct}","");
        split[1] = split[1].replaceAll("j","i");
    		split[1] = split[1].replaceAll("v","u");
	      strArray = split[1].split(" ");
	      int n = strArray.length;
	      /*for(int i=0; i<n;i++){
	        strArray[i] = strArray[i].replaceAll("\\p{Punct}","");
	        strArray[i] = strArray[i].replaceAll("j","i");
	        strArray[i] = strArray[i].replaceAll("v","u");
	      }*/
	      Text word = new Text();
	      /*for(int i=0; i<n;i++){
	        ArrayList<String> word1Fetch = lemmas.get(strArray[i]);
	        if(word1Fetch == null){
	          for(int j=i+1; j<n; j++){
	            ArrayList<String> word2Fetch = lemmas.get(strArray[j]);
	            if(word2Fetch == null){
	              if(strArray[i].length()!=0 && strArray[j].length()!=0){
	                word.set(strArray[i].toLowerCase() + "," + strArray[j].toLowerCase());
	                context.write(word,location);
	              }
	            }
	            else{
	              for(String x: word2Fetch){
	                if(strArray[i].length()!=0 && x.length()!=0){
	                  word.set(strArray[i].toLowerCase() + "," + x.toLowerCase());
	                  context.write(word,location);
	                }
	              }
	            }
	          }
	        }else{
	          //Iterate over all lemmas of word 1 and check for lemmas of word 2 as well.
	          for(String y: word1Fetch){
	            for(int j=i+1; j<n; j++){
	              ArrayList<String> word2Fetch = lemmas.get(strArray[j]);
	              if(word2Fetch == null){
	                if(y.length()!=0 && strArray[j].length()!=0){
	                  word.set(y.toLowerCase() + "," + strArray[j].toLowerCase());
	                  context.write(word,location);
	                }
	              }
	              else{
	                for(String x: word2Fetch){
	                  if(y.length()!=0 && x.length()!=0){
	                    word.set(y.toLowerCase() + "," + x.toLowerCase());
	                    context.write(word,location);
	                  }
	                }
	              }
	            }
	          }
	        }
	      }*/
	      for(int i=0; i<n;i++){
	      	for(int j=i+1; j<n;j++){
	      		for(int k = j+1; k<n; k++){
	      			if(strArray[i].length()!=0 && strArray[j].length()!=0 && strArray[k].length()!=0){
	              word.set(strArray[i].toLowerCase() + "," + strArray[j].toLowerCase() + "," + strArray[k].toLowerCase());
	              context.write(word,location);
	            }
	      		}
	      	}
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
      String[] split = key.toString().split(",");
      ArrayList<String> word1 = lemmas.get(split[0]);
      ArrayList<String> word2 = lemmas.get(split[1]);
      ArrayList<String> word3 = lemmas.get(split[2]);
      if(word1 == null){
      	word1 = new ArrayList<String>();
      	word1.add(split[0]);
      }
      if(word2 == null){
      	word2 = new ArrayList<String>();
      	word2.add(split[1]);
      }
      if(word3 == null){
      	word3 = new ArrayList<String>();
      	word3.add(split[2]);
      }
      Text word = new Text();
      for(String x: word1){
      	for(String y: word2){
      		for(String z: word3){
      			word.set(x+","+y + ","+z);
      			context.write(word, result);
      		}
      	}
      }
      
    }
  }

  public static void main(String[] args) throws Exception {
    //Reading the lemmas file and storing it in the hashmap.
    readLemmas();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word 2 grams");
    job.setJarByClass(Word3grams.class);
    job.setMapperClass(PairMapper.class);
    job.setCombinerClass(LocationReducer.class);
    job.setReducerClass(LocationReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(200);
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