import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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

public class DistanceQuery {
  private  static final int[] queryFileBoW = { 576, 82, 287, 289, 303, 241, 197, 161, 68, 279, 147, 75, 80, 41,
			112, 84, 92, 198, 44, 88, 81, 56, 54, 46, 50, 25, 56, 3, 50, 46, 97, 30, 24, 104, 36, 52, 33, 55, 15,
			32, 43, 30, 25, 46, 20, 25, 2, 61, 11, 63, 21, 9, 26, 9, 7, 48, 11, 27, 19, 4, 7, 20, 5, 45, 15, 13, 30,
			17, 18, 21, 10, 26, 17, 15, 17, 15, 15, 13, 16, 4, 13, 11, 14, 13, 10, 15, 45, 9, 19, 11, 4, 2, 2, 2, 49,
			10, 9, 12, 15, 18 }; // Please do not modify any value in this array
  
  public static int euclid_distance(int[] a,int b [] ){
	  if(a.length!=b.length){
		  return 2147483647;
	  }
	  
	  int result = 0;
	  
	  for(int i=0;i<a.length;i++){
		  result+=(a[i]-b[i])*(a[i]-b[i]);
	  }
	  return result;

  }

  public static int[] strAryToIntAry(String[] str){
	  if(str.length<1){return null;}
	  int [] vector = new int [str.length];
	  for(int i=0;i<vector.length;i++){
  		vector[i]=Integer.parseInt(str[i]);
  	}
	  return vector;
  }
  
  public static class DisantanceMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
       
    //  StringTokenizer itr = new StringTokenizer(value.toString());
  	  String[] keyVal = value.toString().split("\\s");
  	  String[] nums = keyVal[1].split(",");
  	  int [] vector = strAryToIntAry(nums);
  	  word.set("-");
  	  int distance = euclid_distance(vector,queryFileBoW);
  	  Text output = new Text(keyVal[0]+" "+distance);
      context.write(word, output);
    	
    }
  }

  public static class IntDistanceReducer
       extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable(1);
    List<String> distanceList=new ArrayList();
    
    private class KeyValCompare implements Comparator<String> {

		@Override
		public int compare(String kv1, String kv2) {
			// TODO Auto-generated method stub
			int v1 = Integer.parseInt(kv1.split(" ")[1]);
			int v2 = Integer.parseInt(kv2.split(" ")[1]);
			if(v1<v2) return 1;
			return -1;
		}
    	
    }

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
       for (Text val : values) {     
            distanceList.add(val.toString());
       }
       KeyValCompare cmp = new KeyValCompare();
       Collections.sort(distanceList, cmp);
       for(String str : distanceList){
           String[] kv = str.split(" ");
    	   context.write(new Text(kv[0]), new Text(kv[1]));
       }
       
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(DistanceQuery.class);
    job.setMapperClass(DisantanceMapper.class);
    //job.setCombinerClass(IntDistanceReducer.class);
    job.setReducerClass(IntDistanceReducer.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}