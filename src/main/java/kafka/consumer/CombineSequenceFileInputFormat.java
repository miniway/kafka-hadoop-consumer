package kafka.consumer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class CombineSequenceFileInputFormat extends CombineFileInputFormat<LongWritable, Text> {

    public CombineSequenceFileInputFormat() {
		setMaxSplitSize(64*1024*1024);
	}

	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf jobConf,
			Reporter reporter) throws IOException {
		
		
		return new CombineFileRecordReader( jobConf, (CombineFileSplit)split, reporter, 
				CombineSequenceFileReader.class );
	}

	public static class CombineSequenceFileReader implements RecordReader<LongWritable, Text> {
		
		private final SequenceFileRecordReader<LongWritable,Text> delegate ; 
		CombineSequenceFileReader(CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx) throws IOException {
			FileSplit fileSplit = new FileSplit(split.getPath(idx), split.getOffset(idx), split.getLength(idx), split.getLocations());
			delegate = new SequenceFileRecordReader<LongWritable, Text>(conf, fileSplit);
		}
		@Override
		public void close() throws IOException {
			
		}
		@Override
		public LongWritable createKey() {
			return delegate.createKey();
		}
		@Override
		public Text createValue() {
			return delegate.createValue();
		}
		@Override
		public long getPos() throws IOException {
			return delegate.getPos();
		}
		@Override
		public float getProgress() throws IOException {
			return delegate.getProgress();
		}
		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			return delegate.next(key, value);
		}
	}
			
		
}
