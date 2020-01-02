package mimos.ai.nlp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class OutputBolt extends BaseRichBolt {
	private OutputCollector collector;

	private SentenceDetectorME detector;
	@Override
	public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		InputStream inputStream;
		try {
			inputStream = new FileInputStream("/home/siamak/eclipse-workspace/MyStormVersion2.1.0/src/main/resources/de-sent.bin");
			SentenceModel model = new SentenceModel(inputStream);
			 detector = new SentenceDetectorME(model);
		} catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		

	}
	


	@Override
	public void execute(Tuple tuple) {

		
	
		 
		List<String> token= (List<String>) tuple.getValue(2);
		String tokens = String.join(", ", token);
		String textAnnots= (String) tuple.getValue(3);
		  String text= (String) tuple.getValue(1);
		  String id=(String) tuple.getValue(0); 
		  String message="{ \"TWEET\": \""+text+"\", \"TOKENS\":  \""+tokens+"\" , \"ANNOTATED\":  \""+textAnnots+" \" }";

		 
	       collector.emit( new Values(id ,message));
		  
		  collector.ack(tuple);
		  
		  
		 // collector.emit("keywords",new Values(id,allKeywords));
		  //collector.emit(tuple, new Values(allKeywords));
		  
	
		 
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","message"));
	}
}