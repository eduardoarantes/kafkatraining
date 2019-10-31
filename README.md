

# kafka training  
  
Playing around with Kafka and it's main concepts  
  
This will help on the implementation of the new microservice strategy  
  
Topic and consumer groups to replace SNS and SQS solution   
  
It's a more flexible solution   
  
**Important Concepts**  
  

 - **Brokers**
 
	 Kafka  is organized in Brokers, topics and partitions
	 
	 A Broker can hold multiple Partitions of multiple Topics and it's used to allow HA and scalability. 
	 
	 When configuring Partitions for a particular Topic they should be assigned to different Brokers and replicated in 
	 another
	 When a message is send to a particular Topic, Kafka will automatically send to a particular Partition 
	 (lead partition) and replicated it into other Brokers
	 
	 Replication factor dictates the number of copies of a particular Partition into other Brokers
	 
 - **Procucer**
 
	 applications that generate data into a Kafka Topic
	 
 - **Topic**
 
	 It defines destinations of data/messages generate by Producers and consumed by Consumers
	 
 - **Consumer**
 
	 receives data/messages from topics. Each Consumer will read from one or more Partitions
	 
 - **Partitions**
 
	 subdivisions of a Topic that allows parallel consumption of the Topic data. A Partition can only be consumed by one
	 Consumer.
	 The number of Partitions in a Topic dictates the maximum parallel Consumers that can consume the topic.
	 
	 Kafka guarantee that messages in a particular Partition will be delivered in order.
	 
	 That doesn't mean that messages in a Topic will be delivered in order.
	 
	 Data is sent to a Partition using the Round Robin algorithm. It's possible to define a key to force messages about 
	 the same context(Ex: order id, person id) to go to the same Partition guaranting the order of consumption of the 
	 messages in that context. 
	
	 Defining the number of Partitions in a particular Topic is a very important job. It can be changed later, but the 
	 will force the redistribution of the keys and therefore breaking the order of the message in a particular context.
	 As mentioned before, it dictates the maximum number of Consumers
	 
 - **Offset**
 
	 sequential ID of data ingested into each partition. Consumers can read messages by any particular period in time by
	 setting its Offset to a particular ID
	 
	 Each Consumer Group will, by default, start reading from the last Offset

- **Consumer Groups**

	define a set of Consumers to consume messages from a particular Topic via partitions. It doesn't make sense to have 
	more Consumers in a Consumer Group than the number of partitions. The exceeding consumers will be idle
	Consumer Groups have isolated Offset.
	
	If you don't assign a Consumer Group to a particular consumer Kafka will do it for you and this Consumer will be the
	only one in the CG

