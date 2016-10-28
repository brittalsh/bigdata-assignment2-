package code.articles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import code.util.WikipediaPageInputFormat;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 * @author Tuan
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

    	
		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DisßtributedCache here
			super.setup(context);
			//load the people.txt from HDFS
            URI[] cacheFile = context.getCacheFiles();
            BufferedReader sc = new BufferedReader(new FileReader(cacheFile[0].getPath()));
           
        	//put the names from people.txt into Hashset peopleArticlesTitles 
    		String line=null;
    		while((line = sc.readLine() ) != null)
    			peopleArticlesTitles.add(line);
    		sc.close();
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here
			//using the function getTitle from WikipediaPage to get the title of the page
			//if the title appears in the peopleArticlesTitles , write the XMLpage into output by using the function getRawXML
			if(peopleArticlesTitles.contains(inputPage.getTitle()))
				context.write(new Text(""), new Text(inputPage.getRawXML()));
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO: you should implement the Job Configuration and Job call
		// here

		//initializing the job related setups
		
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		Job job = Job.getInstance(conf, "GetArticlesMapred"); 
		job.setJarByClass(GetArticlesMapred.class);
        //set the distributed cache file's path
		job.addCacheFile(new Path("people.txt").toUri());    
		
		//set mapper
		
        job.setMapperClass(GetArticlesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(WikipediaPageInputFormat.class);
        //using WikipediaPageInputFormat as input format
        WikipediaPageInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
	}

        
}
