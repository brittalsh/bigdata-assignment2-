package code.lemma;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import code.util.StringIntegerList;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * 
 *
 */

public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		public static Set<String> stopwords = new HashSet<String>();
		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, StringIntegerList>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			//load the stop_words.txt file from HDFS
            URI[] cacheFile = context.getCacheFiles();
            BufferedReader sc = new BufferedReader(new FileReader(cacheFile[0].getPath()));
            
          //put the stop words from stop_words.txt into Hashset stopwords 
    		String line=null;
    		while((line = sc.readLine() ) != null)
    			stopwords.add(line);
    		sc.close();
		}
		


		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {
			// TODO: implement Lemma Index mapper here
			
			//create a few patterns to remove unrelated informations
			Pattern TAG = Pattern.compile("^\\|.*"); // used to remove xml setup marks like "|style = """
			Pattern NUMBER = Pattern.compile("([a-z]|[A-Z])*[0-9]+([a-z]|[A-Z])*"); // used to remove numbers and the words with numbers (like 20th)
			Pattern rmSignal = Pattern.compile("[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]"); // used to remove all the symbols
			Pattern rmSpace = Pattern.compile("[ ]+"); // used to remove all the redundant spaces
			String content = page.getContent().toString();
			content = TAG.matcher(content).replaceAll(" ");
			content = NUMBER.matcher(content).replaceAll(" ");
			content = rmSignal.matcher(content).replaceAll(" ");
			content = rmSpace.matcher(content).replaceAll(" ");
			
			//using Stanford coreNLP to process the information through functions defined in Tokeizer.java
			//the function tokenize helped to lemmatize and tokenize the string
			//the words after lemmatizing and tokenizing is stored in List<String> splited
			Tokenizer tokenizer = new Tokenizer();
			List<String> splited = tokenizer.tokenize(content);
			
			// we first put all the tokenized words  and its appearence number into a map
			// then transform the map into StringIntergerlist by using the initializing function StringIntergerLsit(map)
			Map<String, Integer> wordcount = new HashMap();
			for (String temp : splited)
			{
				// remove all the upper case words
				temp = temp.toLowerCase();
				// remove the stop words by using the hashset stopwords
				if(!stopwords.contains(temp))
				{
					// count how many times a word appears
					if(wordcount.containsKey(temp))
					{
						//if the word has already appears, the wordcount plus one
						int num = wordcount.get(temp);
						wordcount.put(temp, num+1);
					}
					else
					{
						//if the word first appears, add it into map
						wordcount.put(temp, 1);
					}
				}
			}
		
			context.write(new Text(page.getDocid().toString()), new StringIntegerList(wordcount));
		}
	}
		
    // rewrite the RecordWriter function to output key and value in required form
    public static class LemmaRecordWriter extends RecordWriter<Text, StringIntegerList> {

    	  private PrintWriter out;
    	  private String separator =" ";
    	  public LemmaRecordWriter(FSDataOutputStream fileOut) {
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
    public static class LemmaOutputFormat extends FileOutputFormat<Text, StringIntegerList> {

    	  private String prefix = "custom_";
    	  @Override
    	  public RecordWriter<Text, StringIntegerList> getRecordWriter(TaskAttemptContext job)
    	      throws IOException, InterruptedException {
    	    // new a writable file
    	    Path outputDir = FileOutputFormat.getOutputPath(job);
    	    String subfix = job.getTaskAttemptID().getTaskID().toString();
    	    Path path = new Path(outputDir.toString()+"/"+prefix+subfix.substring(subfix.length()-5, subfix.length()));
    	    FSDataOutputStream fileOut = path.getFileSystem(job.getConfiguration()).create(path);
    	    return new LemmaRecordWriter(fileOut);
    	  }
    	}

}
