package net.javaguides.emailservice.kafka;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class SendEmailConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SendEmailConsumer.class);

  @KafkaListener(topics = "python-events")
  public void sendEmail(Map<String, Object> event) {
    LOGGER.info("Inside sendEmail with event: {}", event);
    LOGGER.info("Sleeping for 2 seconds");
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @KafkaListener(topics = "order_topics")
  public void sendOrderEmail(Map<String, Object> order) {
    LOGGER.info("Inside sendOrderEmail with order: {}", order);
    LOGGER.info("Sleeping for 3 seconds");
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
