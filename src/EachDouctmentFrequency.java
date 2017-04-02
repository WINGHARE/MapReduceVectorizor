import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//first
public class EachDouctmentFrequency {
  


	/**Define Mapper
	 * Input : Documents
	 * Output: <k,v>= <(word,document name),1>**/
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      /*
       * Get Docname
       * */
      String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
      
      /* Split and get words from text files
       * Unwanted terms is replaced and words are converted to lowercase
       * */
      String[] words = value.toString().replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");  
  
      for (String w :words) {
    	if( w.compareTo("")==0) {continue;}
    	context.write(new Text(w+" "+filename.trim()), one);
    	
      }
    }
  }
  /**Define Reducer
	 * Input : <k,v>= <(word,document name),1>
	 * Output: <k,v>= <(word,document name),n>
	 * So Reducer can act as combiner
	 * **/

  public static class CountVectorReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
			throws IOException, InterruptedException {
		int sum = 0;
		
		/*
		 * Perform add and commit reduce
		 * */

		for (IntWritable val : values) {
		
	        sum += val.get();
			
	    }
		
		context.write(key, new IntWritable(sum));
		
	}	  
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "key word count");
    job.setJarByClass(EachDouctmentFrequency.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setCombinerClass(CountVectorReducer.class);
    job.setReducerClass(CountVectorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}