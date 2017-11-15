package twinbrother.de.jms.fruitapp;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class FruitHarvester implements Runnable {

	final Logger logger = LoggerFactory.getLogger(FruitHarvester.class);

	private QueueConnectionFactory conFactory;
	private QueueConnection connection;
	private QueueSession session;
	private Queue fruitQueue;
	private QueueSender sender;

	private final String QUEUE_NAME = "fruit-queue";
	private int harvestPerMinute;
	private final String AWS_CREDENTIALS = "src\\main\\resources\\aws.properties";
	private boolean isStopped = false;

	public FruitHarvester(int harvestPerMinute) {
		this.harvestPerMinute = harvestPerMinute;
	}

	private void init() {
		try {
			conFactory = SQSConnectionFactory.builder().withRegion(Region.getRegion(Regions.US_WEST_2))
					.withAWSCredentialsProvider(new PropertiesFileCredentialsProvider(AWS_CREDENTIALS)).build();

			connection = conFactory.createQueueConnection();
			session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			fruitQueue = session.createQueue(QUEUE_NAME);
			sender = session.createSender(fruitQueue);
			connection.start();

		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void send() {
		try {
			TextMessage msg = session.createTextMessage(grapFruit());
			msg.setStringProperty("Type", "Fruit");
			sender.send(msg);
			logger.info("Sent: {}", msg.getText());
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String grapFruit() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		node.put("fruitType", FruitType.randomFruitType().toString());

		return node.toString();
	}

	private int getDelay() {
		if (harvestPerMinute != 0) {
			int delayInMillS = 60000 / harvestPerMinute;
			return delayInMillS;
		}
		return 0;
	}

	@Override
	public void run() {
		try {
			int delayInMillS = this.getDelay();
			this.init();
			while (!isStopped) {
				this.send();
				TimeUnit.MILLISECONDS.sleep(delayInMillS);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		int harvestPerMinute = 10;
		FruitHarvester harvester = new FruitHarvester(harvestPerMinute);
		Thread harvesterThread = new Thread(harvester);
		harvesterThread.start();
		Scanner scanner = new Scanner(System.in);
		while (!scanner.next().equals("C"))
			;
		scanner.close();
		harvester.setStopped(true);
		try {
			harvesterThread.join();
			System.exit(0);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
