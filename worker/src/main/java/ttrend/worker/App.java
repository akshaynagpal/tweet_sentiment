package ttrend.worker;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import org.json.*;
import com.ibm.watson.developer_cloud.alchemy.v1.AlchemyLanguage;
import com.ibm.watson.developer_cloud.alchemy.v1.model.DocumentSentiment;


class RunThread implements Runnable{

	private Thread t;
	private String threadName;
	private String zookeeper;
	private String topic;
	private final ConsumerConnector consumer;
	private AWSCredentials credentials;
	// Initialize zookeeper properties to read from Kafka
	RunThread(String name, String zookeeper,String topic) {
		threadName = name;
		System.out.println("Creating " +  threadName );
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeper);
		properties.put("group.id", "twitter");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("zookeeper.session.timeout.ms", "500");
		properties.put("zookeeper.sync.time.ms", "250");
		properties.put("auto.commit.interval.ms", "1000");
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
		this.topic = topic;
		// Get AWS creadentials
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (~/.aws/credentials), and is in valid format.",
							e);
		}

	}

	public void testConsumer() {
		// Get the data from Kafka
		Map<String, Integer> topicCount = new HashMap<String,Integer>();
		topicCount.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
		for (final KafkaStream stream : streams) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				String tweet = new String(it.next().message());
				System.out.println("Tweet:" + tweet);
				JSONObject tweetData = new JSONObject(tweet.toString());
				System.out.println("TSENTIMENT:"+tweetData.get("status").toString());
				String sentiment = null;
				try{
// Call alchemy function. Set status to neutral to handle errors like rate limiting,
// too many special characters
					sentiment = getSentiment(tweetData.get("status").toString());
				}catch(Exception e){
					e.printStackTrace();
					sentiment = "neutral";
				}
				
				System.out.println("SENTIMENT:"+ sentiment);
				tweetData.put("sentiment", sentiment);
				tweetData.remove("status");
// Push to SNS
				AmazonSNSClient snsClient = new AmazonSNSClient(credentials);
				snsClient.setRegion(Region.getRegion(Regions.US_EAST_1));
				//publish to an SNS topic
				String topicArn = "arn:aws:sns:us-east-1:021959201754:tweetTrends";
				PublishRequest publishRequest = new PublishRequest(topicArn, tweetData.toString());
				PublishResult publishResult = snsClient.publish(publishRequest);
				//print MessageId of message published to SNS topic
				System.out.println("MessageId - " + publishResult.getMessageId());
			}
		}
		if (consumer != null) {
			consumer.shutdown();
		} 
	}
	// Get the sentiment from Alchemy API
	public String getSentiment(String tweet){
		AlchemyLanguage service = new AlchemyLanguage();
    	Properties prop = new Properties();
    	InputStream input = null;
    	String parsedSentiment = null;
    	try {
    		// get api key
			input = new FileInputStream("config.properties");
			prop.load(input);
			//Alchemy settings
	    	service.setApiKey(prop.getProperty("key"));
	    	Map<String,Object> params = new HashMap<String, Object>();
	    	//string passed to Alchemy
	    	params.put(AlchemyLanguage.TEXT, tweet);
	    	DocumentSentiment sentiment = service.getSentiment(params).execute();
	    	
	    	// parsing json for sentiment
	    	JSONObject obj = new JSONObject(sentiment.toString());
	    	parsedSentiment = obj.getJSONObject("docSentiment").getString("type");
	    	
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return parsedSentiment;
	}

	public void run() {
		testConsumer();
	}

	public void start(){
		System.out.println("Starting " +  threadName);
		if(t == null){
			t = new Thread (this, threadName);
			t.start ();
		}
	}
}


public class App 
{

	public static void main(String[] args) {
		// Start pool thread for workers
		RunThread runThread1 = new RunThread("thread1", "localhost:2181", "tweet");
		runThread1.start();
		RunThread runThread2 = new RunThread("thread2", "localhost:2181", "tweet");
		runThread2.start();
	}	
}
