import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountPerDoc {
	/**Define Mapper
	 * Input : <k,v>= <(word,docname),n>
	 * Output: <k,v>= <docname,(word,n)>
	 * While n is the count of word in article "docname"
	 * **/
    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	/*
         * Mapper emits <docname,(word,n)>
         * Words in the same article will go to the same reducer.
         * Because document name of our data is define as "class (sample_number)"
         * So split function will split the document name into 2 parts（class and sample_number）. 
         * keyVal = {(word,docname),n}
         * newKey = {word,docname}
         * */
      String[] wordDocname_n = value.toString().split("\t"); 
      String[] word_docname = wordDocname_n[0].split(" ");
     
      context.write(new Text(word_docname[1]+" "+word_docname[2]), 
    		        new Text(word_docname[0]+" "+wordDocname_n[1]));
      
    }
  }

  /**Define Reducer
	 * Input : <k,v>= <docname,(word,n)>
	 * Output: <k,v>= <(docname,word),(n,N)>
	 * While N is the length of each article
	 * **/

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int N = 0;
      String docName = key.toString();
      
      /*
       * Hadoop cannot re-iterate Iterable so we cached information
       * Cached map will have a format of <word,n>
       *
       * */
      Map<String,String> localMap = new HashMap<String,String>();
      /*
       * Iterative sum up n which represents the length of each article
       *
       * */
      for (Text val : values) {
    	  String [] word_n = val.toString().split(" ");
    	  int n = Integer.parseInt(word_n[1]);
    	  N += n;
    	  localMap.put(word_n[0], word_n[1]);
      }
      /*
       * Iterative emit <K,v> = <(word,docName),(n,N)>
       *
       * */
      for(Map.Entry<String, String> entry : localMap.entrySet()){//for begins
    	  String word = (String)entry.getKey();
    	  String n = (String)entry.getValue();
    	  context.write(new Text(word+" "+docName), 
    			  new Text(n+" "+N));
	  }//for ends     
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count per doc");
    job.setJarByClass(WordCountPerDoc.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}