package com.sparkexpert;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

public class Main {

	public static void main(String[] args) throws InterruptedException {

		
		twitter4j.conf.ConfigurationBuilder builder=new ConfigurationBuilder();
		  builder.setOAuthAccessToken("935178434300534784-lmCWC4x3OUZI6URDKoGpVjbHJFvWjoY");
		  builder.setOAuthAccessTokenSecret("pARLfyyDsMi9RcejzSQ7xkk3CjYzKWO5a0kQO3w5UcmEC");
		  builder.setOAuthConsumerKey("e3eSc5EcerySMpv7u8pTrd3uk");
		  builder.setOAuthConsumerSecret("kh2Ce1Z0eBf8RzeBD2ezASNRdUeVC8e15RTfd6iJMP1qjaf9EW");
		
		SparkConf sparkConf = new SparkConf().setAppName("Tweets").setMaster("local[*]");
		JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

		JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(sc, new OAuthAuthorization(builder.build()));

		JavaDStream<String> statuses = tweets.map(status -> status.getText());
		
		statuses.print();
		
		sc.start();
		sc.awaitTermination();
	}
}
