package solution;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import solution.CDRConstant;



public class STDSubscribers {
	public static long toMillis(String date)
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date datefrm = null;
		try {
			datefrm = format.parse(date);
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return datefrm.getTime();
	}//END OF FUNCTION
	
	public static class TokenzierMapper extends Mapper<Object, Text, Text, LongWritable>
	{
		Text phoneNumber = new Text();
		LongWritable durationInMinutes = new LongWritable();
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String[] parts = value.toString().split("[|]");
			
			if(parts[CDRConstant.STDFlag].equalsIgnoreCase("1"));
			
			{
				phoneNumber.set(parts[CDRConstant.fromPhoneNumber]);
				String callEndTime = parts[CDRConstant.callEndTime];
				String callStartTime = parts[CDRConstant.callStartTime];
				long duration = toMillis(callEndTime)- toMillis(callStartTime);
				durationInMinutes.set(duration/(1000*60));	
				
				context.write(phoneNumber, durationInMinutes);
			}	
		}
		
	}
	
	public static class SUmReducer extends Reducer <Text, LongWritable, Text, LongWritable>
	{
	private LongWritable result = new LongWritable();
	
	@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)
				throws IOException, InterruptedException {
		
		long sum = 0;
		for (LongWritable value : values)
		{
			sum += value.get();
					
		}
		this.result.set(sum);
		if(sum >= 60)
			context.write(key, this.result);
		
			
	}
}
	
public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
	if(args.length != 2) {
		System.err.print("usage: stdsubscribers <in> <out>");
		System.exit(2);		
	}
		
	Job job = new Job(conf,"STD Subscribers");
	job.setJarByClass(STDSubscribers.class);
	job.setMapperClass(TokenzierMapper.class);
	job.setReducerClass(SUmReducer.class);
	job.setCombinerClass(SUmReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
		
}
}
