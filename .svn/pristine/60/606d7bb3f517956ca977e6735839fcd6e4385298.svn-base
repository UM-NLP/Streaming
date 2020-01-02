package mimos.ai.nlp;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
public class SplitBolt extends ShellBolt implements IRichBolt {

	 	 
	 public SplitBolt(){
	        super("python", "/home/siamak/IdeaProjects/KafkaPython/SentenceSplitterBolt.py");
	    }

@Override
public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// TODO Auto-generated method stub
	
}

@Override
public Map<String, Object> getComponentConfiguration() {
	// TODO Auto-generated method stub
	return null;
}



}