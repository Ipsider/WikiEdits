package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikiEdits {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		// Create StreamExecutionEnvironment in order to set execution parameters and create sources.
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// Create source to read from Wikipedia IRC log.
		DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
		
		// For the purpose of keying this stream on the user names, we have to provide a KeySelector object.
		KeySelector<WikipediaEditEvent, String> key = new KeySelector<WikipediaEditEvent, String>() {
			
	        @Override
	        public String getKey(WikipediaEditEvent event) {
	            return event.getUser();
	        }
	    };
		
	    // Key the stream of WikipediaEditEvent objects, which gives us a stream of key-value-
	    // tuples of a String and a WikipediaEditEvent.
		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(key);
		
		// The identity element of the operation for the fold function.
		Tuple2<String, Long> identityElement = new Tuple2<>("", 0L);
		
		// The fold function gets a WikipediaEditEvent object and the desired output class.
		// A Fold transformation on each window slice for each unique key is performed. In our
		// case we start from an initial value of ("", 0L) and add to it the byte difference of
		// every edit in that time window for a user.
		FoldFunction<WikipediaEditEvent, Tuple2<String, Long>> foldFunction = 
				new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
			
	        @Override
	        public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
	            acc.f0 = event.getUser();
	            acc.f1 += event.getByteDiff();
	            return acc;
	        }
	    };
	    
	    // Slice the stream in non-overlapping windows of five seconds, on which to perform a computation.
	    WindowedStream<WikipediaEditEvent, String, TimeWindow> windowedKeyedEdits = keyedEdits.timeWindow(Time.seconds(5));
	    
	    // Perform fold operation. The resulting Stream now contains a Tuple2<String, Long>
	    // for every user which gets emitted every five seconds.
	    DataStream<Tuple2<String, Long>> results = windowedKeyedEdits
			    .fold(identityElement, foldFunction);
	    
	    // Print the stream to the console.
	    results.print();
	    
	    // Start the actual flink job execution.
	    see.execute();
	}
}
