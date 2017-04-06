import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDF {
	private static final int numberOfDocuments = 1200;
	/**Define Mapper
	 * Input : <k,v>= <(word,docname),(n,N,m)>
	 * Output: <k,v>= <(word,docname),TF-IDF>
	 * While n is the count of word in article "docname"
	 * While N is the length of the document
	 * m is the count of documents contains the word 
	 * **/
    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	/*
         * Mapper emits <(word,docname),TF-IDF>
         * Words in the same article will go to the same reducer.
         * Because document name of our data is define as "class (sample_number)"
         * So split function will split the document name into 2 parts（class and sample_number）. 
         * wordDocname_nNm = {(word,docname),(n,N,m)}
         * n_N_m = {n,N,m}
         * */
      String[] wordDocname_nNm = value.toString().split("\t"); 
      String[] n_N_m = wordDocname_nNm[1].split(" ");
      double n = Double.parseDouble(n_N_m[0]);
      double N = Double.parseDouble(n_N_m[1]);
      double m = Double.parseDouble(n_N_m[2]);
      double TF_IDF = (n/N)*Math.log(numberOfDocuments/m);
     
      context.write(new Text(wordDocname_nNm[0]), 
    		        new DoubleWritable(TF_IDF));
      
    }
  }

  /**Define Reducer
	 * Input : <k,v>= <word,(docname,n,N,1)>
	 * Output: <k,v>= <(word,docname),(n,N,m)>
	 * While m = Sum over 1's
	 * **/

  public static class IntSumReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for(DoubleWritable val:values){
    	  context.write(key, val);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TF-IDF");
    job.setJarByClass(TFIDF.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setMapOutputKeyClass(Text.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}