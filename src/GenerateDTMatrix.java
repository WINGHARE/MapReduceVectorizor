import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenerateDTMatrix {
	private static final int numberOfDocuments = 1200;
	/**Define Mapper
	 * Input : <k,v>= <(docname，word),(tfidf)>
	 * Output: <k,v>= <word,docvector>
	 * **/
    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

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
      String[] wordDocname_TFIDF = value.toString().split("\t"); 
      String[] word_Docname = wordDocname_TFIDF[0].split(" ");
      
      context.write(new Text(word_Docname[0]), 
    		  new Text(word_Docname[1]+" "+wordDocname_TFIDF[1]));
     
       
    }
  }

  /**Define Reducer
	 * Input : <k,v>= <word,(docname,n,N,1)>
	 * Output: <k,v>= <(word,docname),(n,N,m)>
	 * While m = Sum over 1's
	 * **/

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,DoubleArrayWritable> {
	  
	  int parseIndex(String docname){
		 int result = 1201;
		 if(docname.length()<1){return -1;}
		 String[] class_docNum = docname.replaceAll("[^0-9 ]", " ").split(" ");
		 int classNum = Integer.parseInt(class_docNum[0]);
		 int docNum = Integer.parseInt(class_docNum[1]);
		 
		 result = (classNum-1)*200+docNum;
		 return result-1;
	  }

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
      double[] vect = new double[numberOfDocuments]; 
      for(Text val:values){
          String[] docname_TFIDF = val.toString().split(" ");
          double tfIdf= Double.parseDouble(docname_TFIDF[1]);
           try {
        			  int index = parseIndex(docname_TFIDF[0]);
                      vect[index] = tfIdf;
			} catch (Exception e) {
					System.out.println(docname_TFIDF[0]);
			}  	                                   
      }
      
	  context.write(key, new DoubleArrayWritable(vect));

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TF-IDF");
    job.setJarByClass(GenerateDTMatrix.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleArrayWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}