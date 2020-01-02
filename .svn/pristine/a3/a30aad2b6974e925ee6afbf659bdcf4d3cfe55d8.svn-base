package mimos.ai.nlp;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;




public class PipelineTopology {
 public static void main(String args[]) throws Exception {
	 
		/*
		 * Properties props = new Properties(); props.put("metadata.broker.list",
		 * "127.0.0.1:9092"); props.put("bootstrap.servers", "127.0.0.1:9092");
		 * props.put("request.required.acks", "1"); props.put("serializer.class",
		 * "kafka.serializer.StringEncoder");
		 */
	 

	 Config conf = new Config();

	 Properties props = new Properties();
	    props.put("bootstrap.servers", "127.0.0.1:9092");
	    props.put("request.required.acks", "1");
	    props.put("serializer.class", "kafka.serializer.StringEncoder");
	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    conf.put("kafka.broker.config", props);
	    conf.put(KafkaBolt.TOPIC, "OutboundTopic");
	    conf.setDebug(true);
	    conf.put("topology.subprocess.timeout.secs", 1000);

		/*
		 * Properties props = new Properties();props.put("bootstrap.servers",
		 * topoProperties.getProperty("bootstrap.servers"));
		 * props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
		 * io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		 * props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
		 * io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		 * conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
		 */
	    
	    KafkaBolt<String, String> kafkabolt = new KafkaBolt<String, String>()
	            .withTopicSelector(new DefaultTopicSelector("OutboundTopic"))
	            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("id", "message"))
	            //new FieldNameBasedTupleToKafkaMapper("id", "text"+ "pos")
	                        .withProducerProperties(props);
	    
	    POSBoltEnglish posBoltEnglish = new POSBoltEnglish();
	    Map Englishenv = new HashMap();
	    Englishenv.put("PYTHONPATH", "/usr/bin/");
	    Englishenv.put("inputs", "EnglishSentenceBolt");
	    posBoltEnglish.setEnv(Englishenv);
	    
	    POSBoltForeign posBoltForeign = new POSBoltForeign();
	    Map Foreignenv = new HashMap();
	    Foreignenv.put("PYTHONPATH", "/usr/bin/");
	    Foreignenv.put("inputs", "ForeignSentenceBolt");
	    posBoltForeign.setEnv(Foreignenv);
	    
	    SentimentBolt sentimentBolt = new SentimentBolt();
	    Map Sentimentenv = new HashMap();
	    Sentimentenv.put("PYTHONPATH", "/usr/bin/");
	    Sentimentenv.put("inputs", "SentimentBolt");
	    sentimentBolt.setEnv(Sentimentenv);
	    
  


  final TopologyBuilder tp = new TopologyBuilder();

  tp.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("127.0.0.1:9092", "stormtopic").build()), 2);
  tp.setBolt("SentenceBolt", new SentenceBolt(),3).shuffleGrouping("kafka_spout");
  tp.setBolt("TokenBolt",   new TokenBolt() ,3).shuffleGrouping("SentenceBolt");
 
 tp.setBolt("LanguageBolt", new LanguageBolt(),2).shuffleGrouping("TokenBolt");
  
 tp.setBolt("EnglishPosBolt", posBoltEnglish,4).shuffleGrouping("LanguageBolt","streamEnglish");
  
 
 
  tp.setBolt("ForeignPosBolt", posBoltForeign, 4).shuffleGrouping("LanguageBolt","StreamForeign");
  
  tp.setBolt("Output", new OutputBolt(), 2).shuffleGrouping("EnglishPosBolt","default").shuffleGrouping("ForeignPosBolt","default");
  tp.setBolt("forwardToKafka", kafkabolt, 2).shuffleGrouping("Output");
  
  
  LocalCluster localCluster = new LocalCluster();
  try {
  localCluster.submitTopology("wordcounter-topology", conf, tp.createTopology());

   Thread.sleep(100000000);
  
  // localCluster.shutdown();
   
  }
  catch (InterruptedException e) {
   // TODO Auto-generated catch block
   e.printStackTrace();
   localCluster.shutdown();
  }
 }
}