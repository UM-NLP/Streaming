package mimos.ai.nlp;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;


public class POSBoltForeign extends ShellBolt implements IRichBolt {
    
	
	public POSBoltForeign(){
        super("python", "/home/siamak/IdeaProjects/KafkaPython/POSBoltForeign.py");
    }

	

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("id", "text","tokens","pos"));
		// declarer.declareStream("StreamForeign", new Fields("id", "text","pos"));
	}
}