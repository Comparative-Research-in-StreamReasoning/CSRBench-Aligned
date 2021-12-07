/*******************************************************************************
 * Copyright 2013 Politecnico di Milano, Universidad Politécnica de Madrid
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Authors: Daniele Dell'Aglio, Jean-Paul Calbimonte, Marco Balduini,
 * 			Oscar Corcho, Emanuele Della Valle
 ******************************************************************************/
package eu.planetdata.srbench.oracle;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.planetdata.srbench.oracle.io.CSRBenchDataStream;
import eu.planetdata.srbench.oracle.io.DataStream;
import eu.planetdata.srbench.oracle.io.JsonConverter;
import eu.planetdata.srbench.oracle.io.answers.Datatypes.GeneralResult;
import eu.planetdata.srbench.oracle.io.answers.Datatypes.Queries125.*;
import eu.planetdata.srbench.oracle.io.answers.Datatypes.Query3.*;
import eu.planetdata.srbench.oracle.io.answers.Datatypes.Query4.*;
import eu.planetdata.srbench.oracle.io.answers.Datatypes.Query6.*;
import eu.planetdata.srbench.oracle.io.answers.Datatypes.Query7.*;
import eu.planetdata.srbench.oracle.io.answers.Datatypes.Result;
import eu.planetdata.srbench.oracle.io.answers.TransferObject;
import eu.planetdata.srbench.oracle.io.answers.VariableBindings;
import eu.planetdata.srbench.oracle.query.ContinuousQuery;
import eu.planetdata.srbench.oracle.query.WindowDefinition;
import eu.planetdata.srbench.oracle.repository.BenchmarkVocab;
import eu.planetdata.srbench.oracle.repository.SRBenchImporter;
import eu.planetdata.srbench.oracle.result.StreamProcessorOutput;
import eu.planetdata.srbench.oracle.result.StreamProcessorOutputBuilder;
import eu.planetdata.srbench.oracle.result.TimestampedRelation;
import eu.planetdata.srbench.oracle.result.TimestampedRelationElement;
import eu.planetdata.srbench.oracle.s2r.ReportPolicy;
import eu.planetdata.srbench.oracle.s2r.WindowOperator;
import eu.planetdata.srbench.oracle.s2r.WindowScope;
import eu.planetdata.srbench.oracle.utils.Configuration.Config;
import eu.planetdata.srbench.oracle.utils.Configuration.Configuration;
import eu.planetdata.srbench.oracle.utils.QueryInformation.LogicalWindow;
import eu.planetdata.srbench.oracle.utils.QueryInformation.OutputOperator;
import eu.planetdata.srbench.oracle.utils.QueryInformation.Query;
import eu.planetdata.srbench.oracle.utils.QueryInformation.Window;
import eu.planetdata.srbench.oracle.utils.server.FileServer;
import org.apache.commons.io.IOUtils;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Oracle {
	public enum QueryLanguage {
		cqels, csparql, rspql, unsupported
	}
	public enum RDFFormat {
		ntriples, turtle, rdfxml
	}

	private final static Logger logger = LoggerFactory.getLogger(Oracle.class);
	private Repository repo;

	private String configPath = "http://localhost:11111/configuration.json";
	private String answerPath = "http://localhost:11112/answers.json";
	private String resultsPath = "http://localhost:11113/results.html";
	private final String streamURL = "http://localhost:12345/CSRBenchDataStream/datastream#1";
	private boolean detailedResults = false;

	private DataStream dataStream;
	private long t0;
	private List<String> variableNames;
	private FileServer configServer;

	private RDFFormat rdfFormat;
	private int waitingTime = 3000;
	private String engineName;

	public Oracle(){
		repo = new SailRepository(new NativeStore(Config.getInstance().getRepoDir(), "cspo,cops"));
	}

	protected StreamProcessorOutput executeStreamQuery(ContinuousQuery query, long t0, ReportPolicy policy, long lastTimestamp){
		StreamProcessorOutputBuilder ret = new StreamProcessorOutputBuilder(query.getR2SOperator(), Config.getInstance().getEmtpyRelationOutput());
		WindowOperator windower = new WindowOperator(query.getWindowDefinition(), policy, t0);

		try{
			if(!repo.isInitialized())
				repo.initialize();

			RepositoryConnection conn = repo.getConnection();

			WindowScope tr = windower.getNextWindowScope(conn);

			while(tr!=null && tr.getFrom()<=lastTimestamp){
				logger.debug("Block: [{},{})", tr.getFrom(), tr.getTo());
				List<URI> graphs = getGraphsContent(tr);
				if(query.usesStaticData())
					graphs.add(BenchmarkVocab.graphStaticData);
				if(graphs.size()>0){
					TimestampedRelation rel = executeStreamQueryOverABlock(query.getBooleanQuery(), graphs, tr.getTo());
					ret.addRelation(rel);
				} else {
					logger.debug("The content is empty and the non-empty content report policy is {}", policy.isNonEmptyContent());
					if(!policy.isNonEmptyContent()){
						if(Config.getInstance().getEmtpyRelationOutput())
							ret.addRelation(TimestampedRelation.createEmptyRelation(tr.getTo()));
					}
				}
				tr=windower.getNextWindowScope(conn);
			}
		}catch(RepositoryException e){logger.error("Error while retrieving the connection to the RDF store", e);}
		return ret.getOutputStreamResult();
	}

	private TimestampedRelation executeStreamQueryOverABlock(String query, List<URI> graphsList, long computationTimestamp){
		try {
			TimestampedRelation ret = new TimestampedRelation();
			TupleQuery tq;
			tq = repo.getConnection().prepareTupleQuery(org.openrdf.query.QueryLanguage.SPARQL, query);
			DatasetImpl dataset = new DatasetImpl();
			for(URI graph : graphsList){
				dataset.addDefaultGraph(graph);
			}
			tq.setDataset(dataset);
			TupleQueryResult tqr = tq.evaluate();

			if(tqr.hasNext()){
				while(tqr.hasNext()){
					BindingSet bs = tqr.next();
					TimestampedRelationElement srr = new TimestampedRelationElement();
					srr.setTimestamp(computationTimestamp);
					for(String key : tqr.getBindingNames())
						srr.add(key, bs.getBinding(key).getValue());
					ret.addElement(srr);
				}
				return ret;
			}
			else
				return TimestampedRelation.createEmptyRelation(computationTimestamp);

		} catch (RepositoryException e) {
			logger.error("Error while connecting to the repository", e);
		} catch (MalformedQueryException e) {
			logger.error("Malformed query", e);
		} catch (QueryEvaluationException e) {
			logger.error("Error while evaluating the query", e);
		}
		return null;
	}


	private List<URI> getGraphsContent(WindowScope range){
		List<URI> graphsList = new ArrayList<URI>();
		String query = prepareQuery(range);
		logger.trace("Query to retrieve the graphs: {}", query);
		try {
			TupleQuery tq = repo.getConnection().prepareTupleQuery(org.openrdf.query.QueryLanguage.SPARQL, query);
			TupleQueryResult result = tq.evaluate();
			while(result.hasNext()){
				graphsList.add((URI)result.next().getBinding("g").getValue());
				logger.debug("Retrieved graph: {}", graphsList.get(graphsList.size()-1));
			}
			return graphsList;
		} catch (RepositoryException e) {
			logger.error("Error while connecting to the repository", e);
		} catch (MalformedQueryException e) {
			logger.error("Malformed query", e);
		} catch (QueryEvaluationException e) {
			logger.error("Error while evaluating the query", e);
		}
		return null;
	}

	private String prepareQuery(WindowScope range){
		return "SELECT ?g " +
				"FROM <"+BenchmarkVocab.graphList+"> " +
				"WHERE{" +
				"?g <" + BenchmarkVocab.hasTimestamp + "> ?timestamp. " +
				"FILTER(?timestamp >= "+range.getFrom() + " && ?timestamp < "+range.getTo()+") " +
				"}";
	}

	private void parseInputParameters(String[] args) throws Exception {
		HashMap<String, String> parameters = new HashMap<String, String>();
		for (String s : args) {
			parameters.put(s.split("=")[0], s.split("=")[1]);
		}
		setParameters(parameters);
	}

	private void setParameters(Map<String, String> parameters) throws Exception {
		if (parameters.containsKey("queries")) {
			Config.getInstance().setQueries(parameters.get("queries").split(","));
		}
		else {
			throw new Exception("No queries specified.");
		}

		if (parameters.containsKey("queryLanguage")) {
			if (parameters.get("queryLanguage").equals("csparql"))
				Config.getInstance().setQueryLanguage(QueryLanguage.csparql);
			else if (parameters.get("queryLanguage").equals("rspql"))
				Config.getInstance().setQueryLanguage(QueryLanguage.rspql);
			else if (parameters.get("queryLanguage").equals("unsupported"))
				Config.getInstance().setQueryLanguage(QueryLanguage.unsupported);
			else {
				throw new Exception("Query language not supported");
			}
		} else {
			Config.getInstance().setQueryLanguage(QueryLanguage.unsupported);
		}

		if (parameters.containsKey("windowclose")) {
			Config.getInstance().setWindowclose(Boolean.parseBoolean(parameters.get("windowclose")));
		}
		else {
			throw new Exception("windowclose not specified.");
		}
		if (parameters.containsKey("nonemptycontent")) {
			Config.getInstance().setNonemptycontent(Boolean.parseBoolean(parameters.get("nonemptycontent")));
		}
		else {
			throw new Exception("nonemptycontent not specified.");
		}
		if (parameters.containsKey("contentchange")) {
			Config.getInstance().setContentchange(Boolean.parseBoolean(parameters.get("contentchange")));
		}
		else {
			throw new Exception("contentchange not specified.");
		}
		if (parameters.containsKey("emptyrelation")) {
			Config.getInstance().setEmptyrelation(Boolean.parseBoolean(parameters.get("emptyrelation")));
		}
		else {
			throw new Exception("emptyrelation not specified.");
		}
		if (parameters.containsKey("r2SOperator")) {
			if (parameters.get("r2SOperator").equals("RStream"))
				Config.getInstance().setR2SOperator(StreamProcessorOutputBuilder.R2SOperator.Rstream);
			else if (parameters.get("r2SOperator").equals("IStream"))
				Config.getInstance().setR2SOperator(StreamProcessorOutputBuilder.R2SOperator.Istream);
			else if (parameters.get("r2SOperator").equals("DStream"))
				Config.getInstance().setR2SOperator(StreamProcessorOutputBuilder.R2SOperator.Dstream);
			else {
				throw new Exception("r2SOperator not supported");
			}
		}
		else {
			throw new Exception("r2SOperator not specified.");
		}

		if(parameters.containsKey("timeUnit")) {
			Config.getInstance().setTimeUnit(Long.parseLong(parameters.get("timeUnit")));
		}
		if(parameters.containsKey("interval")) {
			Config.getInstance().setInterval(Long.parseLong(parameters.get("interval")));
		}
		if(parameters.containsKey("maxTime")) {
			Config.getInstance().setMaxTime(Integer.parseInt(parameters.get("maxTime")));
		}
		if(parameters.containsKey("excludes")) {
			Config.getInstance().setExcludes(new HashSet<>(Arrays.asList(parameters.get("excludes").split(","))));
		}

		if (parameters.containsKey("configPath")) {
			this.configPath = parameters.get("configPath");
		}

		if (parameters.containsKey("answerPath")) {
			this.answerPath = parameters.get("answerPath");
		}

		if (parameters.containsKey("resultsPath")) {
			this.resultsPath = parameters.get("resultsPath");
		}

		if (parameters.containsKey("rdfFormat")) {
			if (parameters.get("rdfFormat").equals("ntriples"))
				this.rdfFormat = RDFFormat.ntriples;
			else if (parameters.get("rdfFormat").equals("turtle"))
				this.rdfFormat = RDFFormat.turtle;
			else if (parameters.get("rdfFormat").equals("rdfxml"))
				this.rdfFormat = RDFFormat.rdfxml;
			else {
				throw new Exception("RDF format not supported");
			}
		} else
			this.rdfFormat = RDFFormat.turtle;

		if (parameters.containsKey("waitingTime")) {
			this.waitingTime = Integer.parseInt(parameters.get("waitingTime"));
		}

		if (parameters.containsKey("engineName")) {
			this.engineName = parameters.get("engineName");
		}
		else {
			throw new Exception("Engine name not specified");
		}

		if (parameters.containsKey("detailedResults")) {
			this.detailedResults = Boolean.parseBoolean(parameters.get("detailedResults"));
		}
	}

	private void prepareConfiguration() throws Exception {
		File file = new File("configuration.json");
		try (OutputStream out = new FileOutputStream(file)) {
			if(Config.getInstance().getQueryLanguage() == QueryLanguage.unsupported) {
				//Bereitstellen
				out.write(parseToJSON(new Configuration(getUnsupportedQueries(), null)).getBytes(StandardCharsets.UTF_8));
			}
			else {
				out.write(parseToJSON(new Configuration(null, getQueries())).getBytes(StandardCharsets.UTF_8));
			}
		}
		configServer = new FileServer(getPortNumberFromURL(configPath), configPath, file);
		file.delete();
	}

	private String parseToJSON(Object o) {
		Gson gson = new Gson();
		return gson.toJson(o).replaceAll("\\\\r", "\r").replaceAll("\\\\n", "\n").replaceAll("\\\\t", "\t").replaceAll("\\\\u003c", "<").replaceAll("\\\\u003e", ">");
	}

	private Map<String, String> getQueries() {
		Map<String, String> queries = new HashMap<>();
		QueryLanguage queryLanguage = Config.getInstance().getQueryLanguage();
			if(queryLanguage == QueryLanguage.csparql) {
				queries.putAll(getCSPARQLQueries());
			} else if(queryLanguage == QueryLanguage.rspql) {
				queries.putAll(getRSPQLQueries());
		}
		return queries;
	}

	private Map<String, String> getRSPQLQueries() {
		Map<String, String> queries = new HashMap<>();
		for(String queryKey : Config.getInstance().getQuerySet()) {
			queries.put("q" + queryKey.substring(queryKey.length()-1), Config.getInstance().getRSPQLQuery(queryKey));
		}
		return queries;
	}

	private Map<String, String> getCSPARQLQueries() {
		Map<String, String> queries = new HashMap<>();
		for(String queryKey : Config.getInstance().getQuerySet()) {
			queries.put("q" + queryKey.substring(queryKey.length()-1), Config.getInstance().getCSPARQLQuery(queryKey));
		}
		return queries;
	}

	private List<Query> getUnsupportedQueries() throws Exception {
		List<Query> queries = new ArrayList<>();
		for (String queryKey : Config.getInstance().getQuerySet()) {
			List<Window> windows = new ArrayList<>();
			windows.add(windowDefinitionToWindow(Config.getInstance().getWindowDefinition(queryKey)));
			List<String> windowSPARQLQueries = new ArrayList<>();
			windowSPARQLQueries.add(Config.getInstance().getUnsupportedqueryWindow(queryKey));
			Query query = new Query("q" + queryKey.substring(queryKey.length()-1), r2SOperatorToOutputOperator(Config.getInstance().getR2SOperator()),
					Config.getInstance().getUnsupportedquery(queryKey), new ArrayList<>() , windows, windowSPARQLQueries);

			queries.add(query);
		}
		return queries;
	}

	private OutputOperator r2SOperatorToOutputOperator(StreamProcessorOutputBuilder.R2SOperator r2SOperator) {
		switch (r2SOperator) {
			case Istream:
				return OutputOperator.IStream;
			case Dstream:
				return OutputOperator.DStream;
			default:
				return OutputOperator.RStream;
		}
	}

	private void prepareStreams() throws Exception {
		dataStream = new DataStream(getPortNumberFromURL(streamURL), streamURL);
		new Thread(dataStream).start();
		while (dataStream.out == null || dataStream.out.size() == 0) {
			Thread.sleep(20);
		}

		t0 = System.currentTimeMillis();
		System.out.println("T0: " + t0);
		new Thread(new CSRBenchDataStream(streamURL, dataStream, rdfFormat)).start();
	}

	private Window windowDefinitionToWindow(WindowDefinition windowDefinition) {
		return new LogicalWindow(streamURL, (int) windowDefinition.getSize(), (int) windowDefinition.getSlide());
	}


	private void evaluateAnswers() throws InterruptedException, IOException {
		while (!dataStream.stop) {
			Thread.sleep(500);
		}
		System.out.println("Waiting for last answers to be written...");
		Thread.sleep(waitingTime);
		List<TransferObject> answers = loadQueryAnswers();
		File answersAsJSONFile = computeJSONAnswerFile(answers);
		runOracle(answersAsJSONFile);
		answersAsJSONFile.delete();
	}

	private List<TransferObject> loadQueryAnswers() {
		System.out.println("Loading");
		List<TransferObject> re = null;
		try {
			Type collectionType = new TypeToken<List<TransferObject>>(){}.getType();
			re = new Gson().fromJson(IOUtils.toString(new URL(answerPath).openStream(), StandardCharsets.UTF_8), collectionType);
		} catch (IOException e) {
			System.out.println("Answers are not available at " + answerPath);
		} catch (ClassCastException e) {
			System.out.println("Answers are not of type " + List.class);
		}
		return re;
	}

	public File computeJSONAnswerFile(List<TransferObject> answers) {
		GeneralResult generalResult = null;
		String queryId = "";
		Iterator<TransferObject> iterator = answers.iterator();
		while (iterator.hasNext()) {
			TransferObject transferObject = iterator.next();
			if(variableNames == null)
				initializeIndexes(transferObject.getQueryId());

			//-200 is necessary because the results are always produced around XX800 or XX900 and this way we ensure
			//that we got "fitting" timestamps
			long timestamp = ((int) (transferObject.getTimestamp() - t0 - 200) / 1000) * 1000;
			queryId = transferObject.getQueryId();

			for(VariableBindings tsvb : transferObject.getTimestampedVariableBindings()) {
				if (queryId.equals("q1") || queryId.equals("q2") || queryId.equals("q5")) {
					if (generalResult == null)
						generalResult = new GeneralResult125();
					(generalResult).relations.add(new Result125(new Head125(new String[]{variableNames.get(0), variableNames.get(1)}), timestamp, new Results125(new Bindings125[]{new Bindings125(timestamp, new Binding125(new TypeValue("uri", tsvb.getVariableValuesAsString().get(0)), new TypeValue("uri", tsvb.getVariableValuesAsString().get(1))))})));
				} else if (queryId.equals("q3")) {
					if (generalResult == null)
						generalResult = new GeneralResult3();
					(generalResult).relations.add(new Result3(new Head3(new String[]{variableNames.get(0), variableNames.get(1), variableNames.get(2)}), timestamp, new Results3(new Bindings3[]{new Bindings3(timestamp, new Binding3(new TypeValue("uri", tsvb.getVariableValuesAsString().get(0)), new TypeValue("uri", tsvb.getVariableValuesAsString().get(1)), new TripleTypValue("literal", tsvb.getVariableValuesAsString().get(2).split("\"")[1], "http://www.w3.org/2001/XMLSchema#double")))})));
				} else if (queryId.equals("q4")) {
					if (generalResult == null)
						generalResult = new GeneralResult4();
					String value = "";
					if(tsvb.getVariableValuesAsString().get(0).split("\"")[1].equals("0")) {
						value = tsvb.getVariableValuesAsString().get(0).split("\"")[1];
					} else {
						value = tsvb.getVariableValuesAsString().get(0).split("\"")[1].substring(0, tsvb.getVariableValuesAsString().get(0).split("\"")[1].length() - 2);
					}
					(generalResult).relations.add(new Result4(new Head4(new String[]{variableNames.get(0)}), timestamp, new Results4(new Bindings4[]{new Bindings4(timestamp, new Binding4(new TripleTypValue("literal", value, "http://www.w3.org/2001/XMLSchema#double")))})));
				} else if (queryId.equals("q6")) {
					if (generalResult == null)
						generalResult = new GeneralResult6();
					(generalResult).relations.add(new Result6(new Head6(new String[]{variableNames.get(0), variableNames.get(1), variableNames.get(2), variableNames.get(3)}), timestamp, new Results6(new Bindings6[]{new Bindings6(timestamp, new Binding6(new TypeValue("uri", tsvb.getVariableValuesAsString().get(0)), new TypeValue("uri", tsvb.getVariableValuesAsString().get(1)), new TypeValue("uri", tsvb.getVariableValuesAsString().get(3)), new TripleTypValue("literal", tsvb.getVariableValuesAsString().get(2).split("\"")[1], "http://www.w3.org/2001/XMLSchema#double")))})));
				} else {
					if (generalResult == null)
						generalResult = new GeneralResult7();
					(generalResult).relations.add(new Result7(new Head7(new String[]{variableNames.get(0), variableNames.get(1)}), timestamp, new Results7(new Bindings7[]{new Bindings7(timestamp, new Binding7(new TypeValue("uri", tsvb.getVariableValuesAsString().get(0)), new TypeValue("uri", tsvb.getVariableValuesAsString().get(1))))})));
				}
			}
			if(transferObject.getTimestampedVariableBindings().isEmpty()) {
				if (queryId.equals("q1") || queryId.equals("q2") || queryId.equals("q5")) {
					if (generalResult == null)
						generalResult = new GeneralResult125();
					(generalResult).relations.add(new Result125(new Head125(new String[]{variableNames.get(0), variableNames.get(1)}), timestamp, new Results125(new Bindings125[]{})));
				} else if (queryId.equals("q3")) {
					if (generalResult == null)
						generalResult = new GeneralResult3();
					(generalResult).relations.add(new Result3(new Head3(new String[]{variableNames.get(0), variableNames.get(1), variableNames.get(2)}), timestamp, new Results3(new Bindings3[]{})));
				} else if (queryId.equals("q4")) {
					if (generalResult == null)
						generalResult = new GeneralResult4();
					(generalResult).relations.add(new Result4(new Head4(new String[]{variableNames.get(0)}), timestamp, new Results4(new Bindings4[]{})));
				} else if (queryId.equals("q6")) {
					if (generalResult == null)
						generalResult = new GeneralResult6();
					(generalResult).relations.add(new Result6(new Head6(new String[]{variableNames.get(0), variableNames.get(1), variableNames.get(2), variableNames.get(3)}), timestamp, new Results6(new Bindings6[]{})));
				} else {
					if (generalResult == null)
						generalResult = new GeneralResult7();
					(generalResult).relations.add(new Result7(new Head7(new String[]{variableNames.get(0), variableNames.get(1)}), timestamp, new Results7(new Bindings7[]{})));
				}
			}
		}
		File outputFile = new File("./src/main/resources/answers/" + engineName + "-answer-" + queryId.substring(1) + ".json");
		try (OutputStream out = new FileOutputStream(outputFile)) {
			Gson gson = new Gson();
			generalResult.relations = generalResult.relations.stream().distinct().sorted(new Comparator<Result>() {
				@Override
				public int compare(Result result, Result t1) {
					return (int) (result.timestamp - t1.timestamp);
				}
			}).collect(Collectors.toList());
			out.write(gson.toJson(generalResult).getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return outputFile;
	}
	
	private void runOracle(File answerFile) throws IOException, InterruptedException {
		InputStream is = new FileInputStream(answerFile);
		StreamProcessorOutput answer = new JsonConverter().decodeJson(is);

		Writer out = new BufferedWriter(new FileWriter("output.html"));
		out.write("<html><head><title>Oracle Results</title><style type=\"text/css\"> table { border-collapse:collapse; } table,th, td { border: 1px solid black; } </style></head><body>");

		ReportPolicy policy = Config.getInstance().getPolicy();
		for(String queryKey : Config.getInstance().getQuerySet()){
			logger.info("Testing query {}", queryKey);
			out.write("<h2>Query "+queryKey+"</h2>");
			ContinuousQuery query = Config.getInstance().getQuery(queryKey);

			if(detailedResults && answer!=null){
				out.write("<h3>Result of the system</h3>");
				out.write(answer.toString().replaceAll("<", "&lt;").replaceAll("]", "]<br/>"));
			}


			out.write("<h3>Oracle results</h3><table><tr><th>Execution number</th><th>t0</th>"+(detailedResults?"<th>Result</th>":"")+"<th>Matches</th></tr>");
			long actualT0 = query.getFirstT0();
			long lastT0 = query.getFirstT0()+query.getWindowDefinition().getSize();
			long lastTimestamp = Config.getInstance().getInputStreamMaxTime()*Config.getInstance().getInputStreamInterval()+query.getWindowDefinition().getSize();
			logger.info("t0 will vary between {} and {}; the last timestamp will be: {}", new Object[]{actualT0, lastT0, lastTimestamp});
			boolean match = false;
			for(int i=1; !match && actualT0<=lastT0;actualT0+=Config.getInstance().getTimeUnit()){
				logger.info("Execution {}: Window with t0={}", i, actualT0);
				StreamProcessorOutput sr = executeStreamQuery(query, actualT0, policy, lastTimestamp);
				logger.info("Returned result: {}\n", sr);
				out.write("<tr><td>"+i+"</td><td>"+
						actualT0+"</td>" +
						(detailedResults?"<td>"+sr.toString().replaceAll("<", "&lt;").replaceAll("]", "]<br/>")+"</td>":""));
				if(answer!=null){
					match = sr.contains(answer);
					logger.info("The system answer matches: {}", match);
					out.write("<td>"+match+"</td></tr>");
				} else
					out.write("<td>N/A</td></tr>");
				i++;
			}
			out.write("</table>");
		}
		out.write("</body></html>");
		out.close();

		System.out.println("Results available at " + resultsPath);
		FileServer fs = new FileServer(getPortNumberFromURL(resultsPath), resultsPath, new File("output.html"));
		Thread.sleep(	10000);
		fs.stop();
	}

	public static void main(String[] args) throws Exception {
		// Preparing the Repository
		Repository repository = SRBenchImporter.importData(args);

		// Initialize Oracle
		Oracle oracle = new Oracle();

		// Parse Parameters
		oracle.parseInputParameters(args);

		// Prepare Streams & Queries
		oracle.prepareConfiguration();
		oracle.prepareStreams();

		// Evaluate Answers
		repository.shutDown();
		oracle.evaluateAnswers();

		// Stop last Thread
		oracle.configServer.stop();
	}


	private int getPortNumberFromURL(String url) {
		return Integer.parseInt(url.split(":")[2].split("/")[0]);
	}

	private void initializeIndexes(String queryId) {
		variableNames = new ArrayList<>();
		if (queryId.equals("q1") || queryId.equals("q2") || queryId.equals("q5")) {
			variableNames.add("sensor");
			variableNames.add("obs");
		} else if (queryId.equals("q3")) {
			variableNames.add("sensor");
			variableNames.add("obs");
			variableNames.add("value");
		} else if (queryId.equals("q4")) {
			variableNames.add("avg");
		} else if (queryId.equals("q6")) {
			variableNames.add("sensor");
			variableNames.add("ob1");
			variableNames.add("value1");
			variableNames.add("obs");
		} else if (queryId.equals("q7")) {
			variableNames.add("sensor");
			variableNames.add("ob1");
		}
	}
}
