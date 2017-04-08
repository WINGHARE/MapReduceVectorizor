import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable {
	    public DoubleArrayWritable(DoubleWritable[] DoubleWritables) {
	        super(DoubleWritable.class, DoubleWritables);
	    }
	    public DoubleArrayWritable(){
	        super(DoubleWritable.class);

	    }
	    
	    public DoubleArrayWritable(double[] doubles) {
	        super(DoubleWritable.class);
	        DoubleWritable[] DoubleWritables = new DoubleWritable[doubles.length];
	        for (int i = 0; i < doubles.length; i++) {
	        	DoubleWritables[i] = new DoubleWritable(doubles[i]);
	        }
	        set(DoubleWritables);
	    }

	    @Override
	    public String toString() {
	        DoubleWritable[] values = (DoubleWritable[]) super.get();
	        String result="";
	        for(DoubleWritable value :values){
	        	result+=value.toString()+",";  	
	        }
	        return result.substring(0, result.length()-1);
	    }
	    
	  
		public double[] toDoubleArray(){
			Writable[] values=super.get();
	        double [] results = new double[values.length];
	        for(int i=0;i<results.length;i++){
	        	results[i]=((DoubleWritable)values[i]).get();
	        }
	        return results;
	    }
	    
	}   