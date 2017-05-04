package de.unistuttgart.iaas.lcm.jms.helloworld;

import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * A small example that demonstrates how to send and receive messages using JMS
 * with AWS SQS (Amazon Web Service - Simple Queue Service).
 * 
 * @author florian.haupt@iaas.uni-stuttgart.de
 *
 */
public class HelloWorldSQS {

    final static Logger logger = LoggerFactory.getLogger(HelloWorldSQS.class);

    private static QueueConnectionFactory conFactory;
    private static QueueConnection connection;
    private static QueueSession session;
    private static Queue testQueue;
    private static QueueSender sender;
    private static QueueReceiver receiver;

    // name of SQS queue, has to be created in advance 
    private static final String queueName = "jms-test-queue";
    // properties file containing the AWS API credentials (access key and secret key)
    private static final String awsCredentialsFile = "src\\main\\resources\\aws.properties";
    
    public static void main(String[] args) {
	try {
	    // initiate everything we need
	    init();
	    // send a text message to queue
	    send();
	    // receive a text message from queue
	    receive();
	} finally {
	    try {
		connection.close();
	    } catch (JMSException e) {
		e.printStackTrace();
	    }
	}
    }

    private static void init() {
	// http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-java-message-service-jms-client.html
	try {
	    // Create the connection factory using the properties file
	    // credential provider.
	    conFactory = SQSConnectionFactory.builder().withRegion(Region.getRegion(Regions.EU_WEST_1))
		    .withAWSCredentialsProvider(new PropertiesFileCredentialsProvider(awsCredentialsFile)).build();
	    
	    // create connection.
	    connection = conFactory.createQueueConnection();
	    // create session
	    // https://docs.oracle.com/javaee/7/api/javax/jms/Session.html
	    session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
	    // lookup queue (has to be created before, using e.g. the AWS management console)
	    testQueue = session.createQueue(queueName);
	    // create sender and receiver
	    sender = session.createSender(testQueue);
	    receiver = session.createReceiver(testQueue);
	    // start connection (!)
	    connection.start();

	} catch (JMSException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    private static void send() {
	try {
	    // create and send text message
	    TextMessage msg = session.createTextMessage("Hello World!");
	    // set property
	    msg.setStringProperty("sender", "hauptfn");
	    sender.send(msg);
	    logger.debug("Sent message: {}", msg);
	    logger.info("Sent: {}", msg.getText());
	} catch (JMSException e) {
	    e.printStackTrace();
	}
    }

    private static void receive() {
	try {
	    // receive message
	    Message msg = receiver.receive();
	    logger.debug("Received message: {}", msg);
	    if (msg instanceof TextMessage) {
		logger.info("Received: {}", ((TextMessage) msg).getText());
		@SuppressWarnings("unchecked")
		Enumeration<String> propNames = msg.getPropertyNames(); 
		while (propNames.hasMoreElements()) {
		    String pn = propNames.nextElement();
		    logger.info("- {} = {}", pn, msg.getObjectProperty(pn));
		}
	    }
	    // required when session is set to Session.CLIENT_ACKNOWLEDGE
	    //msg.acknowledge();
	} catch (JMSException e) {
	    e.printStackTrace();
	}
    }

}