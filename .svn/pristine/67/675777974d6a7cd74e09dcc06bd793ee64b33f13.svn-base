package mimos.ai.nlp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;




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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.util.CoreMap;
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

public class LanguageBolt extends BaseRichBolt {
	private OutputCollector collector;

	private InputStream modelIn = null;
	private LanguageDetectorModel trainedModel = null;
	@Override
	public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			modelIn = new FileInputStream("/home/siamak/eclipse-workspace/MyStormVersion2.1.0/src/main/resources/langdetect-183.bin");
		
			trainedModel = new LanguageDetectorModel(modelIn);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
	
	@Override
	public void execute(Tuple tuple) {
		
		 List<String> tokens= (List<String>) tuple.getValue(2);
		 String text= (String) tuple.getValue(1);
		  String id=(String) tuple.getValue(0); 
		LanguageDetectorME myCategorizer = new LanguageDetectorME(trainedModel);
		Language language = myCategorizer.predictLanguage(text);
		//System.out.println("Best language: " + language.getLang());
		//System.out.println("Best language confidence: " + language.getConfidence());
 if (language.getLang().equals("eng"))
 {	
	 collector.emit("streamEnglish", new Values(id ,text, tokens));
	 collector.ack(tuple);
 }
 else 
 { collector.emit("StreamForeign", new Values(id ,text, tokens));
 	collector.ack(tuple);
 }
		
		 
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("streamEnglish",new Fields("id","text","tokens"));
		declarer.declareStream("StreamForeign", new Fields("id","text","tokens"));
	}
}
