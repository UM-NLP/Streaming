package mimos.ai.nlp;



import com.johnsnowlabs.nlp.DocumentAssembler;
import com.johnsnowlabs.nlp.annotators.LemmatizerModel;
import com.johnsnowlabs.nlp.annotators.Tokenizer;
import com.johnsnowlabs.nlp.embeddings.EmbeddingsHelper;
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline;
import com.johnsnowlabs.nlp.pretrained.ResourceDownloader;


public class Sentiment {
	 private static final long serialVersionUID = 1l;
	public static void main(String[] args) {
		// SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		/*
		 * SparkSession spark = SparkSession .builder() .appName("Spark NLP")
		 * .master("local[4]") .config("spark.jars.packages",
		 * "JohnSnowLabs:spark-nlp:2.2.4") .getOrCreate();
		 */
	PretrainedPipeline pipeline = new PretrainedPipeline("explain_document_ml","en", "public/models");
	//DocumentAssembler document_assembler = (DocumentAssembler) new DocumentAssembler().setInputCol("text").setOutputCol("document");
	pipeline.annotate("My name is ali");
	System.out.print(pipeline.annotate("My name is ali"));
		        
		
	}

}
