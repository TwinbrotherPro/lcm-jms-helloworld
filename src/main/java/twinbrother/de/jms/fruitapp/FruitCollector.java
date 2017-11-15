package twinbrother.de.jms.fruitapp;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FruitCollector implements Runnable {

	final Logger logger = LoggerFactory.getLogger(FruitCollector.class);

	private QueueConnectionFactory conFactory;
	private QueueConnection connection;
	private QueueSession session;
	private Queue fruitQueue;
	private QueueReceiver receiver;
	private AsyncReceiver asyncReceiver;

	private final String QUEUE_NAME = "fruit-queue";
	private int collectsPerMinute;
	private final String AWS_CREDENTIALS = "src\\main\\resources\\aws.properties";
	private boolean isStopped = false;

	public FruitCollector(int collectsPerSecond) {
		this.collectsPerMinute = collectsPerSecond;
	}

	private void init() {
		try {
			conFactory = SQSConnectionFactory.builder().withRegion(Region.getRegion(Regions.US_WEST_2))
					.withAWSCredentialsProvider(new PropertiesFileCredentialsProvider(AWS_CREDENTIALS)).build();

			connection = conFactory.createQueueConnection();
			session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			fruitQueue = session.createQueue(QUEUE_NAME);
			receiver = session.createReceiver(fruitQueue);
			// Remove for using Sync Receiver
			asyncReceiver = new AsyncReceiver(getDelay());
			receiver.setMessageListener(asyncReceiver);
			//
			connection.start();

		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void receiveFruit() {
		Message msg;
		try {
			msg = receiver.receive();

			logger.debug("Received message: {}", msg);
			if (msg instanceof TextMessage) {
				TextMessage textMsg = (TextMessage) msg;
				JsonNode json = parseFruitMessage(textMsg.getText());
				logger.info("Received: {}", (json.get("fruitType")));

				@SuppressWarnings("unchecked")
				Enumeration<String> propNames = msg.getPropertyNames();
				while (propNames.hasMoreElements()) {
					String pn = propNames.nextElement();
					logger.info("- {} = {}", pn, msg.getObjectProperty(pn));
				}
			}
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private JsonNode parseFruitMessage(String message) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode jsonObject = mapper.readTree(message);
			return jsonObject;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null; // TODO Better exception handling
	}

	private int getDelay() {
		if (collectsPerMinute != 0) {
			int delayInMillS = 60000 / collectsPerMinute;
			return delayInMillS;
		}
		return 0;
	}

	// Use for sync receiver
	// @Override
	// public void run() {
	// try {
	// this.init();
	// while (!isStopped) {
	// this.receiveFruit();
	// TimeUnit.MILLISECONDS.sleep(this.getDelay());
	// }
	// } catch (InterruptedException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// } finally {
	// try {
	// this.connection.close();
	// } catch (JMSException e) {
	// e.printStackTrace();
	// }
	// }
	//
	// }

	// Use for async receiver
	@Override
	public void run() {
		try {
			this.init();
			while (!isStopped)
				;
		} finally {
			try {
				this.connection.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean isStopped() {
		return isStopped;
	}

	public void setStopped(boolean isStopped) {
		this.isStopped = isStopped;
	}

	public static void main(String[] args) {
		int collectsPerMinute = 1;
		FruitCollector collector = new FruitCollector(collectsPerMinute);
		Thread collectorThread = new Thread(collector);
		collectorThread.start();
		Scanner scanner = new Scanner(System.in);
		while (!scanner.next().equals("C"))
			;
		scanner.close();
		collector.setStopped(true);
		try {
			collectorThread.join();
			System.exit(0);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
