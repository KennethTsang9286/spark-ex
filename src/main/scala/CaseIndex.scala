import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.File
import scala.collection.mutable.HashMap
import xml._
import scala.sys.process._
import scalaj.http._
import play.api.libs.json._

// file - {name, AustLII, catchphrases, sentences, entities}

object CaseIndex {
	// initialize constants
	val ElasticSearchURL = "http://localhost:9200"
	val CoreNLPURL = "http://localhost:9000/"
	val IndexName = "legal_idx"
	val MappingName = "cases"
	val TargetEntityType : List[String] = List("PERSON", "LOCATION", "ORGANIZATION") 
	val Usage : String = "arg[0] == <path to input directory>"
	def main(args: Array[String]) {

		if(args.size != 1){
			throw new IllegalStateException("Invalid args. " + Usage);
		}

		// make path to directory from args(0)
		val inputDir = new File(args(0))
		// get path to all files in the directory
		val unfilteredFiles = getListOfFiles(inputDir)
		
		// Create a Scala Spark Context.
		val conf = new SparkConf().setAppName("assignment3").setMaster("local")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// initialize index and mapping in elastic search
		initElasticSearch()

		// filter to reserve only xml files
		val files = unfilteredFiles.filter((f)=>(f.getName().substring(f.getName().length() - 4) == ".xml"))
		// read xml files
		val xmls : Seq[Elem] = files.map(scala.xml.XML.loadFile(_))
		
		// extract text, location entity, person entity and organization entity from xmls
		val dataForElasticSearch = xmls.map(extractDictFromXML(_))
		
		// add name to xmlData
		(dataForElasticSearch zip files).foreach{
			case (d, f) => d.put("name", Seq(f.getName().substring(0, f.getName().length() - 4)))
		}

		// post data to ElasticSearch
		dataForElasticSearch.foreach((data) => postDataToElasticSearch(data))
		
		// stop spark context
    	sc.stop()
		
	}

	// post data to elastic search 
	def postDataToElasticSearch(input : HashMap[String, Seq[String]]) {
		val filename = input.get("name").head.head
		// make url
		val url : String = Seq(ElasticSearchURL, IndexName, MappingName, filename + "?pretty" ).mkString("/")
		// make payload json data of the form {id, text, location, organization, person}
		val data : String = Json.toJson(HashMap(
			"id" -> input.get("name"), 
			"text" -> input.get("text"),
			"location" -> input.get("location"),
			"organization" -> input.get("organization"),
			"person" -> input.get("person")
		)).toString
		// send data
		val res = Http(url).postData(data).header("content-type", "application/json").asString
		// print to show successful creation
		if (res.code == 200 || res.code == 201){
			println("created " + filename)
		}
		
	}	

	// get list of files from input directory
	def getListOfFiles(dir: File):List[File] = dir.listFiles.filter(_.isFile).toList

	// extract text and entities from xml
	def extractDictFromXML(xml : Elem) : HashMap[String, Seq[String]] = {
		val texts : Seq[String] = (xml.child).map(x => x.text.trim)
		// get entities from corenlp with getEntities
		val entities = texts.map(y => getEntities(y)).flatten
		val location : Seq[String] = entities.filter(x => x(0) == "LOCATION").map(entity => entity(1))
		val person : Seq[String] = entities.filter(x => x(0) == "PERSON").map(entity => entity(1))
		val organization : Seq[String] = entities.filter(x => x(0) == "ORGANIZATION").map(entity => entity(1))
		val output : HashMap[String, Seq[String]] = HashMap("text" -> texts.filter(_.nonEmpty), "location" -> location, "person" -> person, "organization" -> organization )
		return output
	}
	
	// initialize ElasticSearch
	def initElasticSearch(){
		val  initIndiceUrl = Seq(ElasticSearchURL, IndexName + "?pretty").mkString("/")
		
		val initMappingUrl = Seq(ElasticSearchURL, IndexName, MappingName, "_mapping?pretty").mkString("/")
		
		val initMappingData = Json.toJson(HashMap(MappingName -> HashMap("properties" -> HashMap(
			"id" -> HashMap("type" -> "text"),
			"text" -> HashMap("type" -> "text"),
			"location" -> HashMap("type" -> "text"),
			"person" -> HashMap("type" -> "text"),
			"organization" -> HashMap("type" -> "text")
		)))).toString
		
		// initialize indice
		val resIndice = Http(initIndiceUrl).method("put").asString
		// initialize mapping
		val resMapping = Http(initMappingUrl).params("include_type_name" -> "true").postData(initMappingData).header("content-type", "application/json").asString
		
		if( resIndice.code == 200 ){
			println("Created index")
		}

		if ( resMapping.code == 200 ){
			println("Created mapping")
		}
	}

	// get entities in text from corenlp
	def getEntities(text : String) : List[Seq[String]] = {
		
		val properties = Json.toJson(HashMap(
			"annotators" -> "ner",
			"outputFormat" -> "json",
			"ner.applyNumericClassifiers" -> "false",
			"ner.applyFineGrained" -> "false",
			"ner.useSUTime" -> "false"
		)).toString
		
		val data = Json.toJson(HashMap(
			"data" -> text
		)).toString
		
		// get response
		val resEntity = Http(CoreNLPURL).params("properties" -> properties).postData(data).header("content-type", "application/json").asString
		// get sentences from response
		val entityObjects = (Json.parse(resEntity.body) \ "sentences" \ 0 \ "tokens").as[List[JsValue]]
		// entities = [[entityType, text],...]
		val unfilteredEntities = entityObjects.map(x => List( (x \ "ner").as[String] ,(x \ "originalText").as[String]))
		// filter out entities not included in [person, organization, location]
		val entities = unfilteredEntities.filter(x => TargetEntityType.contains(x.head)).map(entity => Seq(entity(0), entity(1)))
		// return entities
		return entities
	}
}