package twinbrother.de.jms.fruitapp;

import java.beans.ExceptionListener;
import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AsyncReceiver implements MessageListener{

	final Logger logger = LoggerFactory.getLogger(AsyncReceiver.class);
	private int delayInMills;
	
	public AsyncReceiver(int delayInMillS) {
		this.delayInMills  = delayInMillS;
	}
	
	@Override
	public void onMessage(Message msg) {
		try {
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
			TimeUnit.MILLISECONDS.sleep(delayInMills);
		} catch (JMSException | InterruptedException e) {
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

}
