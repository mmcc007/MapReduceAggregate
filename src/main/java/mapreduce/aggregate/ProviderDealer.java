package mapreduce.aggregate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProviderDealer extends Configured implements Tool {
	private enum ReducerValue{NewCarCount, UsedCarCount, FailedCount, SucceededCount};
	

	static public class ProviderDealerMapper extends
			Mapper<Object, Text, ProviderDealerWriteable, Text> {
		private enum TrueVehicle {
			ProviderId, DealerId, VIN, Make, Model, ModelYear, Trim, Description, New_Used, Price, Status
		};

		private ProviderDealerWriteable reducerKey;
		private Text reducerValue = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<Integer> counts = new ArrayList<Integer>();
			for (int i = 0; i < ReducerValue.values().length; i++) {
				counts.add(i, new Integer(0));
			}
			String[] tokens = value.toString().split("\\t");
			
			if (tokens[TrueVehicle.New_Used.ordinal()].equals("Used"))
				counts.set(ReducerValue.UsedCarCount.ordinal(), 1);
			else
				counts.set(ReducerValue.NewCarCount.ordinal(), 1);
			
			if (tokens[TrueVehicle.Status.ordinal()].equals("Succeeded"))
				counts.set(ReducerValue.SucceededCount.ordinal(), 1);
			else
				counts.set(ReducerValue.FailedCount.ordinal(), 1);

			reducerKey = new ProviderDealerWriteable(Long.parseLong(tokens[TrueVehicle.ProviderId.ordinal()]), Long.parseLong(tokens[TrueVehicle.DealerId.ordinal()]));
			
			String cnts = new String();
			for (Integer cnt:counts)
				cnts += cnt + " ";
			
			reducerValue=new Text(cnts.trim());
			context.write(reducerKey, reducerValue);
		}
	}

	static public class ProviderDealerReducer extends
			Reducer<ProviderDealerWriteable, Text, ProviderDealerWriteable, Text> {

		@Override
		protected void reduce(ProviderDealerWriteable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long[] accumulator = new long[ReducerValue.values().length];
			for (Text count : values) {
				String[] tokens = count.toString().split("\\s+");
				for (int i = 0; i < tokens.length; i++)
					accumulator[i] += Long.parseLong(tokens[i]);
			}
			String totalStr = new String();
			for (int i = 0; i < accumulator.length; i++)
				totalStr += accumulator[i] + " ";
			context.write(key, new Text(totalStr.trim()));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		Job job = new Job(configuration, "Provider Dealer");
		job.setJarByClass(ProviderDealer.class);

		job.setMapperClass(ProviderDealerMapper.class);
		job.setCombinerClass(ProviderDealerReducer.class);
		job.setReducerClass(ProviderDealerReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}

}
