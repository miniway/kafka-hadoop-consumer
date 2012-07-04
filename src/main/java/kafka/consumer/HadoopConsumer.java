package kafka.consumer;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopConsumer extends Configured implements Tool {

	static {
		Configuration.addDefaultResource("core-site.xml");
		//Configuration.addDefaultResource("mapred-site.xml");
	}
	
	public static class KafkaMapper extends Mapper<LongWritable, BytesWritable, LongWritable, Text> {
		@Override
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException {
			try {
				context.write(key, new Text(new String(value.getBytes())));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public int run(String[] args) throws Exception {
		
	    //ToolRunner.printGenericCommandUsage(System.err);
	    /*
		if (args.length < 2) {
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		*/

        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();
        
        CommandLine cmd = parser.parse(options, args);
        
        //HelpFormatter formatter = new HelpFormatter();
        //formatter.printHelp( "kafka.consumer.hadoop", options );
        
		Configuration conf = getConf();
		conf.set("kafka.topic", cmd.getOptionValue("topic", "test"));
		conf.set("kafka.groupid", cmd.getOptionValue("consumer-group", "test_group"));
		conf.set("kafka.zk.connect", cmd.getOptionValue("zk-connect", "localhost:2182"));
		conf.setInt("kafka.limit", Integer.valueOf(cmd.getOptionValue("limit", "-1")));
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		
		Job job = new Job(conf, "Kafka.Consumer");
		job.setJarByClass(getClass());
		job.setMapperClass(KafkaMapper.class);
		// input
		job.setInputFormatClass(KafkaInputFormat.class);
		// output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(KafkaOutputFormat.class);
		
		job.setNumReduceTasks(0);
		
		KafkaOutputFormat.setOutputPath(job, new Path(cmd.getArgs()[0]));
		
		boolean success = job.waitForCompletion(true);
		if (success) {
		    commit(conf);
		}
		return success ? 0: -1;
	}

    private void commit(Configuration conf) throws IOException {
        ZkUtils zk = new ZkUtils(conf);
        try {
            String topic = conf.get("kafka.topic");
            String group = conf.get("kafka.groupid");
            zk.commit(group, topic);
        } catch (Exception e) {
            rollback();
        } finally {
            zk.close();
        }
    }

    private void rollback() {
    }

    @SuppressWarnings("static-access")
    private Options buildOptions() {
        Options options = new Options();
        
        options.addOption(OptionBuilder.withArgName("topic")
                            .withLongOpt("topic")
                            .hasArg()
                            .withDescription("kafka topic")
                            .create("t"));
        options.addOption(OptionBuilder.withArgName("groupid")
                .withLongOpt("consumer-group")
                .hasArg()
                .withDescription("kafka consumer groupid")
                .create("g"));
        options.addOption(OptionBuilder.withArgName("zk")
                .withLongOpt("zk-connect")
                .hasArg()
                .withDescription("ZooKeeper connection String")
                .create("z"));
        
        options.addOption(OptionBuilder.withArgName("limit")
                .withLongOpt("limit")
                .hasArg()
                .withDescription("kafka limit")
                .create("l"));

        return options;
    }
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HadoopConsumer(), args);
		System.exit(exitCode);
	}

}
