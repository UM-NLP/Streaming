package mimos.ai.nlp;


import com.johnsnowlabs.nlp.DocumentAssembler;
import com.johnsnowlabs.nlp.LightPipeline;
import com.johnsnowlabs.nlp.annotators.LemmatizerModel;
import com.johnsnowlabs.nlp.annotators.Tokenizer;
import com.johnsnowlabs.nlp.embeddings.EmbeddingsHelper;
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TokenBolt extends BaseRichBolt  {
	private OutputCollector collector;
	 SparkSession spark;
	 Pipeline pipeline;
	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		 DocumentAssembler document = new DocumentAssembler();
	        document.setInputCol("text");
	        document.setOutputCol("document");
		Tokenizer tokenizer = new Tokenizer();
        tokenizer.setInputCols(new String[] {"document"});
        tokenizer.setOutputCol("token");

        pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[] {document, tokenizer});

      spark = com.johnsnowlabs.nlp.SparkNLP.start();
	}

	@Override
	public void execute(Tuple input) {
		 String inputText= (String) input.getValue(1);
		  String id=(String) input.getValue(0); 
       
      

        

        LinkedList<String> text = new java.util.LinkedList<String>();

        text.add(inputText);

        Dataset<Row> data = spark.createDataset(text, Encoders.STRING()).toDF("text");

        PipelineModel pipelineModel = pipeline.fit(data);

       // Dataset<Row> transformed = pipelineModel.transform(data);
       //transformed.show();


      

		
		  LightPipeline lightPipeline = new LightPipeline(pipelineModel);
		  
		  java.util.Map<String, java.util.List<String>> result =
		  lightPipeline.annotateJava(inputText);
		  List<String> finalTokens=new ArrayList<String>();
		 finalTokens=result.get("token");
		  collector.emit( new Values(id ,inputText,finalTokens));
	  
		  collector.ack(input);
		/*
		 * java.util.ArrayList<String> list = new java.util.ArrayList<String>();
		 * list.add("Peter is a good person."); list.add("Roy lives in Germany.");
		 * 
		 * System.out.println(lightPipeline.annotateJava(list));
		 */

     

		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare( new Fields("id" ,"inputText","finalTokens"));
	}
}