package mapreduce.aggregate;

import mapreduce.aggregate.ProviderDealer;
import mapreduce.aggregate.ProviderDealerWriteable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ProviderDealerTest {
   MapReduceDriver<Object, Text, ProviderDealerWriteable, Text, ProviderDealerWriteable, Text> mapReduceDriver;
   MapDriver<Object, Text, ProviderDealerWriteable, Text> mapDriver;
   ReduceDriver<ProviderDealerWriteable, Text, ProviderDealerWriteable, Text> reduceDriver;

   @Before
   public void setUp() {
      ProviderDealer.ProviderDealerMapper mapper = new ProviderDealer.ProviderDealerMapper();
      ProviderDealer.ProviderDealerReducer reducer = new ProviderDealer.ProviderDealerReducer();
      mapDriver = new MapDriver<Object, Text, ProviderDealerWriteable, Text>();
      mapDriver.setMapper(mapper);
      reduceDriver = new ReduceDriver<ProviderDealerWriteable, Text, ProviderDealerWriteable, Text>();
      reduceDriver.setReducer(reducer);
      mapReduceDriver = new MapReduceDriver<Object, Text, ProviderDealerWriteable, Text, ProviderDealerWriteable, Text>();
      mapReduceDriver.setMapper(mapper);
      mapReduceDriver.setReducer(reducer);
   }

   @Test
   public void testMapper() {
      mapDriver.withInput(new LongWritable(1), new Text("1	123	1G1RB6E40DU1XXXXX	Chevrolet	Volt	2013	Some trim	Some description	Used	29995	Succeeded"));
      mapDriver.withOutput(new ProviderDealerWriteable(1, 123), new Text("0 1 0 1"));
      mapDriver.runTest();
   }

   @Test
   public void testReducer() {
      List<Text> texts = new ArrayList<Text>();
      texts.add(new Text("0 1 0 1"));
      texts.add(new Text("0 1 0 1"));
      reduceDriver.withInput(new ProviderDealerWriteable(1, 123), texts);
      reduceDriver.withOutput(new ProviderDealerWriteable(1, 123), new Text("0 2 0 2"));
      reduceDriver.runTest();
   }

   @Test
   public void testMapReduce() {
      mapReduceDriver.withInput(new LongWritable(1), new Text("1	123	1G1RB6E40DU1XXXXX	Chevrolet	Volt	2013	Some trim	Some description	Used	29995	Succeeded"));
      mapReduceDriver.withInput(new LongWritable(1), new Text("1	123	1G1RB6E40DU1XXXXX	Chevrolet	Volt	2013	Some trim	Some description	New	29995	Failed"));
      mapReduceDriver.withInput(new LongWritable(1), new Text("2	123	1G1RB6E40DU1XXXXX	Chevrolet	Volt	2013	Some trim	Some description	New	29995	Failed"));
      mapReduceDriver.addOutput(new ProviderDealerWriteable(1, 123), new Text("1 1 1 1"));
      mapReduceDriver.addOutput(new ProviderDealerWriteable(2, 123), new Text("1 0 1 0"));
      mapReduceDriver.runTest();
   }
}
