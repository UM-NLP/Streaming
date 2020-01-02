package mimos.ai.nlp;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.util.CoreMap;
public class LemmaBolt implements IBasicBolt {
 /**
  * 
  */
 private static final long serialVersionUID = -4130092930769665618L;
 private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(LemmaBolt.class);
	private Annotator lemmatizer;

	

 public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

  this.collector=collector;
	
 }

 public void execute(Tuple input) {
	 long startTime = System.nanoTime();
		log.info("Entering lemma bolt");
		
		ArrayList<Annotation> textAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		ArrayList<Multiset<String>> allKeywords=new ArrayList<Multiset<String>>();
		for (int i=0;i<textAnnots.size();i++){
			lemmatizer.annotate(textAnnots.get(i));

			Multiset<String> keywords=HashMultiset.create();
			List<CoreMap> sentences = textAnnots.get(i).get(CoreAnnotations.SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
			  for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
				  String tag=token.get(PartOfSpeechAnnotation.class);
				  if (tag.equals("FW") || tag.startsWith("VB") || tag.startsWith("NN")){
					  String lemma=token.get(LemmaAnnotation.class);
					  if (lemma.length()>2)
						  keywords.add(lemma);
				  }	
	            }
			}
			allKeywords.add(keywords);
		}
		
		log.info("Sending keywords");
		Long estimatedTime = System.nanoTime() - startTime;
		collector.emit("keywords",new Values(id,allKeywords));
		
		collector.ack(input);
 }
 @Override
 public void cleanup() {

 }
 @Override
 public void declareOutputFields(OutputFieldsDeclarer declarer) {
  // TODO Auto-generated method stub
	 declarer.declare(new Fields("id", "allKeywords"));
 }
 @Override
 public Map < String, Object > getComponentConfiguration() {
  // TODO Auto-generated method stub
  return null;
 }
@Override
public void prepare(Map<String, Object> topoConf, TopologyContext context) {
	// TODO Auto-generated method stub
	
}
@Override
public void execute(Tuple input, BasicOutputCollector collector) {
	// TODO Auto-generated method stub
	
}
}