import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//first
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
	
	
	/**Define Writable Subclass **/
	public static class IntArrayWritable extends ArrayWritable {
		    public IntArrayWritable(IntWritable[] intWritables) {
		        super(IntWritable.class, intWritables);
		    }
		    public IntArrayWritable(){
		        super(IntWritable.class);

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
		    public String toString() {
		        IntWritable[] values = (IntWritable[]) super.get();
		        String result="";
		        for(IntWritable value :values){
		        	result+=value.toString()+",";  	
		        }
		        return result.substring(0, result.length()-1);
		    }
		    
		  
			public int[] toIntArray(){
				Writable[] values=super.get();
		        int [] results = new int[values.length];
		        for(int i=0;i<results.length;i++){
		        	results[i]=((IntWritable)values[i]).get();
		        }
		        return results;
		    }
		    
		}   
	
	/**Define Vector Add **/

  public static int[] vector_add(int a [],int b []){
		  if(a.length!=b.length){
			  return null;
		  }
		  
		  int result [] = new int[a.length];
		  
		  for(int i=0;i<a.length;i++){
			  result[i]=a[i]+b[i];
		  }
		  return result;
	  }

	/**Define Mapper
	 * Input : Documents
	 * Output: <k,v>= <docname,vector>**/
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntArrayWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      /*
       * Get Docname
       * */
      String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
      
      /* Split and get words from text files
       * Unwanted terms is replaced and words are converted to lowercase
       * */
      String[] words = value.toString().replaceAll("[^a-zA-Z0-9 ]", " ").toLowerCase().split("\\s+");  
      
      /*
       * Search if words are in the top list
       * If true, initialized a new vector and add a count 
       * Then commit map
       * <docname,vector>
       * */
      for (String w :words) {
    	if( w.compareTo("")==0) {continue;}
    	if(top100Word.contains(w)==true){
			int[] temp = new int[100];
			int index = top100Word.indexOf(w);			
			temp[index]+=1;
    		context.write(new Text(filename.trim()), new IntArrayWritable(temp));
    	}
      }
    }
  }
  /**Define Reducer
	 * Input : <k,v>= <docname,vector>
	 * Output: <k,v>= <docname,vector>
	 * So Reducer can act as combiner
	 * **/

  public static class CountVectorReducer extends Reducer<Text,IntArrayWritable,Text,IntArrayWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntArrayWritable> values,Context context) 
			throws IOException, InterruptedException {
		int [] sum = new int[100];
		
		/*
		 * Perform vecrot_add and commit reduce
		 * */

		for (IntArrayWritable val : values) {
		
	        sum = vector_add(sum,val.toIntArray());
			
	    }
		
		context.write(key, new IntArrayWritable(sum));
		
	}	  
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "key word count");
    job.setJarByClass(KeyWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputValueClass(IntArrayWritable.class);
    
    job.setCombinerClass(CountVectorReducer.class);
    job.setReducerClass(CountVectorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntArrayWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}