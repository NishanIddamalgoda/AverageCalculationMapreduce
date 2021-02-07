package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class AverageCounter {
	
	public static class AvgMaper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			IntWritable number = new IntWritable(Integer.parseInt(value.toString()));
			double random = Math.random();
						
			Text one = new Text("1");
			Text two = new Text("2");
			Text three = new Text("3");
			
			if (random < 0.33) {
				context.write(one, number);
			} else if (random < 0.66) {
				context.write(two, number);
			} else {
				context.write(three, number);
			}	
		}		
	}
	
	public static class AvgReducer extends Reducer<Text, IntWritable, FloatWritable, IntWritable>{
		
		public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException{
			
			int count = 0;
			float sum = 0;
			
			for (IntWritable value : ones) {
                sum += value.get();
                count = count + 1;
            }
			
			FloatWritable sumOutput = new FloatWritable(sum);
			IntWritable countOutput = new IntWritable(count);			
			context.write(sumOutput, countOutput);		
		}		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		 
		 if(otherArgs.length != 2)
		 {
			 System.err.println("* * * Needs two arguments....usage : Avg <input_file> <output_folder>");
			 System.exit(2);
		 }
		 
		Job job = Job.getInstance(conf, "Avg Cal");
		job.setJarByClass(AverageCounter.class);		
		job.setMapperClass(AvgMaper.class);;		
		job.setReducerClass(AvgReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(IntWritable.class);		
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		boolean status = job.waitForCompletion(true);
		
		if(status) {
			System.exit(0); // Exit with Success code	
			System.out.println("Completed the Map Reduce Successfully");
		}
		else
		{
			System.exit(1); // Exit with Failure code # 1
		}
	}

}
