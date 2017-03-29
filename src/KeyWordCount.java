import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

public class KeyWordCount {
  
	/*private final static String[] top100Word = { "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
			"it", "for", "not", "on", "with", "he", "as", "you", "do", "at", "this", "but", "his", "by", "from", "they",
			"we", "say", "her", "she", "or", "an", "will", "my", "one", "all", "would", "there", "their", "what", "so",
			"up", "out", "if", "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time", "no",
			"just", "him", "know", "take", "people", "into", "year", "your", "good", "some", "could", "them", "see",
			"other", "than", "then", "now", "look", "only", "come", "its", "over", "think", "also", "back", "after",
			"use", "two", "how", "our", "work", "first", "well", "way", "even", "new", "want", "because", "any",
			"these", "give", "day", "most", "us" };*/
	
	private final static List<String> top100Word = Arrays.asList("the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
			"it", "for", "not", "on", "with", "he", "as", "you", "do", "at", "this", "but", "his", "by", "from", "they",
			"we", "say", "her", "she", "or", "an", "will", "my", "one", "all", "would", "there", "their", "what", "so",
			"up", "out", "if", "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time", "no",
			"just", "him", "know", "take", "people", "into", "year", "your", "good", "some", "could", "them", "see",
			"other", "than", "then", "now", "look", "only", "come", "its", "over", "think", "also", "back", "after",
			"use", "two", "how", "our", "work", "first", "well", "way", "even", "new", "want", "because", "any",
			"these", "give", "day", "most", "us");

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntArrayWritable>{

    private final static IntWritable one = new IntWritable(1);
    
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	

      String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
      String[] words = value.toString().replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");  
      for (String w :words) {
    	if( w.compareTo("")==0) {continue;}
    	if(top100Word.contains(w)==true){
			int[] temp = new int[100];
			int index = top100Word.indexOf(w);			
			temp[index]+=1;
    		context.write(new Text(filename), new IntArrayWritable(temp));
    	}
      }
    }
  }
  
  public static class IntArrayWritable extends ArrayWritable {
	    public IntArrayWritable(IntWritable[] intWritables) {
	        super(IntWritable.class, intWritables);
	    }
	    
	    public IntArrayWritable(int[] ints) {
	        super(IntWritable.class);
	        IntWritable[] intWritables = new IntWritable[ints.length];
	        for (int i = 0; i < ints.length; i++) {
	        	intWritables[i] = new IntWritable(ints[i]);
	        }
	        set(intWritables);
	    }

	    @Override
	    public IntWritable[] get() {
	        return (IntWritable[]) super.get();
	    }

	    @Override
	    public void write(DataOutput arg0) throws IOException {
	        for(IntWritable data : get()){
	            data.write(arg0);
	        }
	    }
	    @Override
	    public String toString() {
	        IntWritable[] values = get();
	        String result="";
	        for(IntWritable value :values){
	        	result+=value.toString()+",";  	
	        }
	        return result.substring(0, result.length()-1);
	    }
	    
	}   
    
  public static class IntSumCombiner
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  public static class CountVectorReducer extends Reducer<Text,IntArrayWritable,Text,IntArrayWritable> {
	  private Map<String,int[]> localMap = new HashMap<String,int[]>();

	@Override
	protected void reduce(Text key, Iterable<IntArrayWritable> values,Context context) 
			throws IOException, InterruptedException {
		int sum =0;
		/*for (IntArrayWritable val : values) {
	        sum += val.get();
	    }*/
		String keys[] = key.toString().split(" ");
		
		if(localMap.containsKey(keys[0])== false){
			int[] temp = new int[100];
			int index = top100Word.indexOf(keys[1]);			
			temp[index]+=sum;
			localMap.put(keys[0], temp);			
		}else{
			int[] temp = localMap.get(keys[0]);
			int index = top100Word.indexOf(keys[1]);			
			temp[index]+=sum;
			localMap.put(keys[0], temp);	
		}
		for(Map.Entry<String, int[]> entry : localMap.entrySet()){//for begins
			//Text k = new Text(entry.getKey());
			IntArrayWritable a = new IntArrayWritable(entry.getValue());
		  	context.write(new Text(entry.getKey()),a);
		}//for ends
		
	}	  
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "key word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(CountVectorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}