package code.inverted;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import code.lemma.LemmaIndexMapred;
import code.util.StringIntegerList;
import code.util.StringIntegerList.StringInteger;
import code.util.WikipediaPageInputFormat;




/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */



public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, StringInteger> {

		@Override
		public void map(LongWritable articleId, Text indices, Context context) throws IOException,
				InterruptedException {
			// TODO: You should implement inverted index mapper here
			
			//get one line from input,
			//for the input is in the form: doc_id <word, wordcount>,<word, wordcount>
			//we first splited it with " " to get the doc_id, then splited the remaining string with "<",">","," to get the word and wordcount
			String tuple = indices.toString();
			String[] splited= tuple.split("\t| ",2);
			if(splited.length > 1)
			{
				String[] pairs = splited[1].split("<|>|,");
				for (int i = 0; i < pairs.length; )
				{
					if(pairs[i].length() > 0)
					//for the split function may cause a few "", we only need to consider the meaningful splited string
					{
						int count = Integer.parseInt(pairs[i+1]);
						//due to the split function, the wordcount always follows the word
						StringInteger temp = new StringInteger(splited[0], count);
						//build the StringInteger pair with word and wordcount
						context.write(new Text(pairs[i]), temp);
						i = i + 2;
					}
					else
						i++;
				}
			}
			
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement inverted index reducer here
			// the reducer got the iterable<StringInteger> and transform it into StringIntegerList
			// we first put all the values in reducer value_list into a map
			// then transform the map into StringIntergerlist by using the initializing function StringIntergerLsit(map)
			Map<String, Integer> doclist = new HashMap();
			for(StringInteger x : articlesAndFreqs)
			{
				doclist.put(x.getString(), x.getValue());
			}
			StringIntegerList result = new StringIntegerList(doclist);
			context.write(lemma, result);
		}
	}
	
    // rewrite the RecordWriter function to output key and value in required form
    public static class InvertedRecordWriter extends RecordWriter<Text, StringIntegerList> {

    	  private PrintWriter out;
    	  private String separator =" ";
    	  public InvertedRecordWriter(FSDataOutputStream fileOut) {
    	    out = new PrintWriter(fileOut);
    	  }

    	  @Override
    	  public void write(Text key, StringIntegerList value) throws IOException,
    	      InterruptedException {
    			out.println(key.toString()+separator+value.toString());
    	  }

    	  @Override
    	  public void close(TaskAttemptContext context) throws IOException,
    	      InterruptedException {
    	    out.close();
    	  }

    	}

    //Setup a CustomOutputFormat to realize required output format
    public static class InvertedOutputFormat extends FileOutputFormat<Text, StringIntegerList> {

    	  private String prefix = "custom_";
    	  @Override
    	  public RecordWriter<Text, StringIntegerList> getRecordWriter(TaskAttemptContext job)
    	      throws IOException, InterruptedException {
    	    // new a writable file
    	    Path outputDir = FileOutputFormat.getOutputPath(job);
    	    String subfix = job.getTaskAttemptID().getTaskID().toString();
    	    Path path = new Path(outputDir.toString()+"/"+prefix+subfix.substring(subfix.length()-5, subfix.length()));
    	    FSDataOutputStream fileOut = path.getFileSystem(job.getConfiguration()).create(path);
    	    return new InvertedRecordWriter(fileOut);
    	  }
    	}

	public static void main(String[] args) {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf1 = new Configuration();
		
		Job job1;
		try {
			GenericOptionsParser gop1 = new GenericOptionsParser(conf1, args);
			String[] otherArgs = gop1.getRemainingArgs();
			
			//initializing the LemmaIndexMapred job related setups
			
		    job1 = Job.getInstance(conf1, "LemmaIndexMapred");
			job1.setJarByClass(LemmaIndexMapred.class);
			//set the distributed cache file's path
			job1.addCacheFile(new Path("stop_words.txt").toUri());     
			//set the LemmaIndexMapred Mapper
			job1.setMapperClass(LemmaIndexMapred.LemmaIndexMapper.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(StringIntegerList.class);
			job1.setInputFormatClass(WikipediaPageInputFormat.class);
			job1.setOutputFormatClass(LemmaIndexMapred.LemmaOutputFormat.class);
			//using WikipediaPageInputFormat as input format
			WikipediaPageInputFormat.addInputPath(job1, new Path(otherArgs[0]));
			//using rewritten output format LemmaOutputForma as output format
			LemmaIndexMapred.LemmaOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
			job1.waitForCompletion(true);
		
			
			//initializing the InvertedIndexMapred job related setups
			
			Job job2;
		    // InvertedIndexMapred job using the same configuration as LemmaIndexMapred job in order to get the permission to access cluster
			job2 = Job.getInstance(conf1, "InvertedIndexMapred");
			//Set InvertedIndexMapred Mapper
			job2.setJarByClass(InvertedIndexMapred.class);
			job2.setMapperClass(InvertedIndexMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(StringInteger.class);
			//InvertedIndexMapred Reducer
			job2.setReducerClass(InvertedIndexReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(StringIntegerList.class);
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
			//using rewritten output format InvertedOutputForma as output format
            InvertedOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			job2.waitForCompletion(true);
			
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		
		
	}
		
}
