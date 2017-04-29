package de.unistuttgart.iaas.lcm.jms.helloworld;

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
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small example that demonstrates how to send and receive messages using JMS
 * and JNDI with Apache ActiveMQ.
 * 
 * @author florian.haupt@iaas.uni-stuttgart.de
 *
 */
public class HelloWorldActiveMQ {

	final static Logger logger = LoggerFactory.getLogger(HelloWorldActiveMQ.class);

	private static QueueConnectionFactory conFactory;
	private static QueueConnection connection;
	private static QueueSession session;
	private static Queue testQueue;
	private static QueueSender sender;
	private static QueueReceiver receiver;

	public static void main(String[] args) {
		try {
			// initiate everything we need
			init();
			// send a text message to "TestQueue"
			send();
			// receive a text message from "TestQueue"
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
		try {
			// reads the configuration from jndi.properties file
			Context jndi = new InitialContext();

			// connect to messaging system
			conFactory = (QueueConnectionFactory) jndi.lookup("HelloWorldFactory");
			connection = conFactory.createQueueConnection();
			// create session
			session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			// lookup queue
			testQueue = (Queue) jndi.lookup("TestQueue");
			// create sender and receiver
			sender = session.createSender(testQueue);
			receiver = session.createReceiver(testQueue);
			// start connection (!)
			connection.start();

		} catch (NamingException e) {
			e.printStackTrace();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private static void send() {
		try {
			// create and send text message
			TextMessage msg = session.createTextMessage("Hello World!");
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
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

}