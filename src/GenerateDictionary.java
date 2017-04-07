import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenerateDictionary {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

	private static final int numberOfDocuments = 1200;

    private  List<String> localList = new ArrayList<String>();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] wordDocname_nNm = value.toString().split("\t"); 
    	String[] word_Docname = wordDocname_nNm[0].split(" ");
    	
    	if(localList.contains(word_Docname[0])==false){
    		localList.add(word_Docname[0]);
    		String[] n_N_m = wordDocname_nNm[1].split(" ");
    	    double m = Double.parseDouble(n_N_m[2]);
    	    if(1<=m && m<= 0.8 * numberOfDocuments){
    	      	  context.write(new Text("-"), 
    	      		        new Text(word_Docname[0]));
    	   }
    	}       
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      List<String> unsorted = new ArrayList<String>();
      for (Text val : values) {
        unsorted.add(val.toString());
      }
      java.util.Collections.sort(unsorted);

      for(String str : unsorted){
    	  context.write(key, new Text(str));
      }
      
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(GenerateDictionary.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}