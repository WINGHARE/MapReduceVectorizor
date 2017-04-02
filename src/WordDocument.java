import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordDocument {
	/**Define Mapper
	 * Input : <k,v>= <(word,docname),(n,N)>
	 * Output: <k,v>= <word,(docname,n,N,1)>
	 * While n is the count of word in article "docname"
	 * **/
    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	/*
         * Mapper emits <(word,docname),(n,N)>
         * Words in the same article will go to the same reducer.
         * Because document name of our data is define as "class (sample_number)"
         * So split function will split the document name into 2 parts（class and sample_number）. 
         * keyVal = {(word,docname),(n,N)}
         * newKey = {word,docname}
         * */
      String[] wordDocname_nN = value.toString().split("\t"); 
      String[] word_docname = wordDocname_nN[0].split(" ");
     
      context.write(new Text(word_docname[0]), 
    		        new Text(word_docname[1]+" "+word_docname[2]+" "+wordDocname_nN[1]+" "+one));
      
    }
  }

  /**Define Reducer
	 * Input : <k,v>= <word,(docname,n,N,1)>
	 * Output: <k,v>= <(word,docname),(n,N,m)>
	 * While m = Sum over 1's
	 * **/

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int m = 0;
      String word = key.toString();
      
      /*
       * Hadoop cannot re-iterate Iterable so we cached information
       * Cached map will have a format of <word,n>
       *
       * */
      Map<String,String> localMap = new HashMap<String,String>();
      /*
       * Iterative sum up m which represents the #docs containing the word
       *
       * */
      for (Text val : values) {
    	  String [] docname_n_N_1 = val.toString().split(" ");
    	  int i = Integer.parseInt(docname_n_N_1[4]);
    	  m += i;
    	  String n = docname_n_N_1[2];
    	  String N = docname_n_N_1[3];
    	  String docname_n_N_m = new String(n+" "+N+" "+m);
    	  localMap.put(docname_n_N_1[0]+docname_n_N_1[1], docname_n_N_m);
      }
      /*
       * Iterative emit <K,v> = <(word,docName),(n,N,m)>
       *
       * */
      for(Map.Entry<String, String> entry : localMap.entrySet()){//for begins
    	  String docName = (String)entry.getKey();
    	  String docName_n_N_m = (String)entry.getValue();
    	  context.write(new Text(word+" "+docName), 
    			  new Text(docName_n_N_m));
	  }//for ends     
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count per doc");
    job.setJarByClass(WordDocument.class);
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