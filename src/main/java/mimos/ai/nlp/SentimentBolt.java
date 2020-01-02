package mimos.ai.nlp;
import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;


public class SentimentBolt extends ShellBolt implements IRichBolt {
    
	
	public SentimentBolt(){
        super("python", "/home/siamak/IdeaProjects/KafkaPython/SentimentBolt.py");
    }

	

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("id", "text","pos"));
		// declarer.declareStream("streamEnglish",new Fields("id", "text","pos"));
	}
}