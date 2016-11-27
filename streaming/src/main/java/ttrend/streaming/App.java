package ttrend.streaming;

import java.util.Map;
import java.util.Properties;

import io.searchbox.client.JestClient;
import kafka.producer.KeyedMessage;
import twitter4j.JSONObject;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.ConfigurationBuilder;

public class App 
{
	private static final String twitterKey = "VNLXiEY0l2XbFzSxJJz9AyGuA";
	private static final String twitterSecret = "YXdAiSN2FhtM1BLxzfZTHvgGBAbCvIUsvE32i7TQpr85meLa12";

	private static final int numberOfTweets = 50;

	private static String[] searchTerms = new String[] {"love", "work", "food", "travel", "trump", "dog"};

	/* 
	 * Get the Oauth2 token from Twitter
	 */

	public static OAuth2Token getToken()
	{
		OAuth2Token token = null;
		ConfigurationBuilder configurationBuilder;

		configurationBuilder = new ConfigurationBuilder();
		configurationBuilder.setApplicationOnlyAuthEnabled(true);

		configurationBuilder.setOAuthConsumerKey(twitterKey).setOAuthConsumerSecret(twitterSecret);

		try
		{
			token = new TwitterFactory(configurationBuilder.build()).getInstance().getOAuth2Token();
		}
		catch (Exception e)
		{
			System.out.println("Error while retrieving OAuth2 token");
			e.printStackTrace();
			System.exit(0);
		}

		return token;
	}

	/*
	 * Get a fully application-authenticated Twitter object useful for making subsequent calls.
	 */
	public static Twitter getTwitter()
	{
		OAuth2Token token;

		//	First step, get a "bearer" token that can be used for our requests
		token = getToken();

		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

		configurationBuilder.setApplicationOnlyAuthEnabled(true);

		configurationBuilder.setOAuthConsumerKey(twitterKey);
		configurationBuilder.setOAuthConsumerSecret(twitterSecret);

		configurationBuilder.setOAuth2TokenType(token.getTokenType());
		configurationBuilder.setOAuth2AccessToken(token.getAccessToken());

		//	return Twitter Object
		return new TwitterFactory(configurationBuilder.build()).getInstance();
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args){

		long maxID = -1;
		Twitter twitter = getTwitter();
		JestClient client = null;
		kafka.javaapi.producer.Producer<String, String> producer = null;
		try
		{
			Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("search");

			//	Get rate limit for our API
			RateLimitStatus tweetLimit = null;
			try{
				tweetLimit = rateLimitStatus.get("/search/tweets");
			}catch(Exception e){
				System.out.println(e.getMessage());
			}

			Properties properties = new Properties();			
			properties.put("metadata.broker.list", "localhost:9092");
			properties.put("serializer.class", "kafka.serializer.StringEncoder");
			properties.put("request.required.acks", "1");

			kafka.producer.ProducerConfig config = new kafka.producer.ProducerConfig(properties);
			producer = new kafka.javaapi.producer.Producer<String, String>(config);

			//	Run in an infinite loop if no errors while sleeping in between for rate limits
			for (int queryNumber=0;queryNumber<100000; queryNumber++)
			{

				for(int ctr=0; ctr<6 ;ctr++){

					System.out.println("Starting loop:"+ queryNumber);

					//	Check if hitting limit
					if (tweetLimit.getRemaining() == 0)
					{
						//	Sleep when yes
						System.out.println("Sleeping for "+ tweetLimit.getSecondsUntilReset()+
								" seconds because of rate limits");

						Thread.sleep((tweetLimit.getSecondsUntilReset()+500) * 1000l);
					}

					Query q = new Query(searchTerms[ctr]);			
					q.setCount(numberOfTweets);				
					q.setResultType(Query.RECENT);			
					q.setLang("en");						

					// Make sure not getting duplicate tweets
					if (maxID != -1)
					{
						q.setMaxId(maxID - 1);
					}

					// Run tweet query
					QueryResult result = null;
					try{
						result = twitter.search(q);
					}catch(Exception e){
						System.out.println(e.getMessage());
						tweetLimit = result.getRateLimitStatus();
						continue;			
					}

					//Check if no tweets returned
					if (result.getTweets().size() == 0)
					{
						tweetLimit = result.getRateLimitStatus();
						continue;			
					}

					//Process tweets
					for (Status status: result.getTweets())			
					{

						//	Keep track of the lowest tweet ID
						if (maxID == -1 || status.getId() < maxID)
						{
							maxID = status.getId();
						}
						if(status.getGeoLocation()!=null){
							System.out.println("At "+status.getCreatedAt().toString()+ " name: "+status.getUser().getScreenName()+
									" tweeted: "+status.getText()+" from: "+status.getGeoLocation().toString());
						 
							JSONObject tweetData = new JSONObject();
							tweetData.put("topic", searchTerms[ctr]);
							tweetData.put("id", status.getId());
							tweetData.put("status", status.getText());
							tweetData.put("lat", status.getGeoLocation().getLatitude());
							tweetData.put("long", status.getGeoLocation().getLongitude());
							String tweet = tweetData.toString();
							KeyedMessage<String, String> data = new KeyedMessage<String, String>("tweet", tweet);
							producer.send(data);
						}
					}

					//Get rate limit before next call
					tweetLimit = result.getRateLimitStatus();
				}
			}
			producer.close();	
		}catch(Exception e){
			e.printStackTrace();
			producer.close();	
		}	
		

	} 	

}
