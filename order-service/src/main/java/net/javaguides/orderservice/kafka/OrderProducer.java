package net.javaguides.orderservice.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import net.javaguides.basedomains.dto.OrderEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OrderProducer.class);

  @Autowired private NewTopic topic;

  @Autowired private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

  @Autowired private ObjectMapper objectMapper;

  public void sendMessage(OrderEvent orderEvent) {
    LOGGER.info("Order event received: {}", orderEvent);

    Map<String, Object> payload = objectMapper.convertValue(orderEvent, Map.class);

    Message<Map<String, Object>> message = MessageBuilder
        .withPayload(payload)
        .setHeader(KafkaHeaders.TOPIC, topic.name())
        .build();

    kafkaTemplate.send(message);
  }
}
