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

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] keyVal = value.toString().split("\t"); 
      String[] newKey = keyVal[0].split(" ");
     /* StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	System.out.println(value);
        word.set(itr.nextToken());
        context.write(word, new Text("Text"));
      }*/
      context.write(new Text(newKey[1]+" "+newKey[2]), 
    		        new Text(newKey[0]+" "+keyVal[1]));
      
    }
  }

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
      
      for (Text val : values) {
    	  String [] word_n = val.toString().split(" ");
    	  int n = Integer.parseInt(word_n[1]);
    	  N += n;
    	  localMap.put(word_n[0], word_n[1]);
      }
      
    //  values.iterator().
      for(Map.Entry<String, String> entry : localMap.entrySet()){//for begins
    	  String word = (String)entry.getKey();
    	  String n = (String)entry.getValue();
    	  context.write(new Text(word+" "+docName), 
    			  new Text(n+" "+N));
	  	  //context.write((Text)entry.getKey(),new IntWritable((int)entry.getValue()));
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