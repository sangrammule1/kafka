package com.ct.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.ct.entity.Topic;

import io.swagger.annotations.ApiOperation;

/**
 * this class is used to communicate with kafka
 * 
 * @author Sangram
 *
 */
@RestController
public class TopicController {

	Logger logger = LoggerFactory.getLogger(TopicController.class);

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	Topic topic = null;

	/**
	 * this method is used to get the Topic from kafka
	 * 
	 * @return Topic obj
	 */
	@GetMapping("/getTopic")
	@ApiOperation(value = "Get the topic", response = Topic.class)
	public ResponseEntity<Topic> consumeTopic() {
		logger.debug("consume topic method execution started");
		logger.debug("consume topic method execution ended");
		return new ResponseEntity<Topic>(topic, HttpStatus.OK);
	}

	@KafkaListener(topics = "myTopic", groupId = "group-id", containerFactory = "topicKafkaListenerContainerFactory")
	public void getTopic(Topic topic) {
		logger.info("Topic is: " + topic);
		this.topic = topic;
	}

	/**
	 * this method is used to produce obj on kafka
	 * 
	 * @param topic
	 * @return string
	 */
	@PostMapping("/saveTopic")
	@ApiOperation(value = "Produce the topic", response = String.class)
	public ResponseEntity<String> produceTopic(@RequestBody Topic topic) {
		logger.debug("produceTopic method execution is started");
		kafkaTemplate.send("myTopic", topic);
		logger.info("Topic is producede to kafka");
		logger.debug("produceTopic method execution is ended");
		return new ResponseEntity<String>("Topic is saved", HttpStatus.CREATED);

	}
	
	/**
	 * this method is used to update obj on kafka
	 * 
	 * @param topic
	 * @return string
	 */
	@PutMapping("/updateTopic")
	@ApiOperation(value = "Update the topic", response = String.class)
	public ResponseEntity<String> updateTopic(@RequestBody Topic topic) {
		logger.debug("updateTopic method execution is started");
		kafkaTemplate.send("myTopic", topic);
		logger.info("Topic is updated in kafka");
		logger.debug("updateTopic method execution is ended");
		return new ResponseEntity<String>("Topic is updated", HttpStatus.CREATED);

	}
	
	
	
	/**
	 * this method is used to delete obj in kafka
	 * 
	 * @return string
	 */
	@DeleteMapping("/deleteTopic")
	@ApiOperation(value = "To delete the topic")
	public ResponseEntity<String> deleteTopic(){
		logger.debug("deleteTopic method execution is started");
		logger.debug("deleteTopic method execution is ended");
		return new ResponseEntity<String>("Topic is deleted",HttpStatus.OK);
	}

}
