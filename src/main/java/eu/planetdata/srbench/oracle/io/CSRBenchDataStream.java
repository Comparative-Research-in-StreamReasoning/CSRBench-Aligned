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
package eu.planetdata.srbench.oracle.io;


import eu.planetdata.srbench.oracle.Oracle;
import eu.planetdata.srbench.oracle.repository.BenchmarkVocab;
import eu.planetdata.srbench.oracle.utils.Configuration.Config;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CSRBenchDataStream implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(CSRBenchDataStream.class);
	private String stream_uri;
	private int max=0;
	private long interval=0;
	private Set<String> exclude=null;
	private static boolean stop = false;
	public static Map<String, Long> obsTimestamps = new HashMap<>();
	private DataStream stream;
	private Oracle.RDFFormat rdfFormat;


	public CSRBenchDataStream(String uri, DataStream stream, Oracle.RDFFormat rdfFormat){
		stream_uri = uri;
		max=Config.getInstance().getInputStreamMaxTime();
		interval=Config.getInstance().getInputStreamInterval();
		exclude=Config.getInstance().getInputStreamHoles();
		this.stream = stream;
		this.rdfFormat = rdfFormat;
	}
	
	public void addTimestampedData(File f, long timestamp) throws Exception {
		long currentTimestamp = timestamp + System.currentTimeMillis();
		URI graph = BenchmarkVocab.getGraphURI(timestamp);
		Model model1 = ModelFactory.createDefaultModel();
		model1.createLiteralStatement(new ResourceImpl(graph.toString()), new PropertyImpl(BenchmarkVocab.hasTimestamp.toString()), System.currentTimeMillis());
		Model model2 = RDFDataMgr.loadModel(f.getAbsolutePath());

		StringWriter s = new StringWriter();
		if(rdfFormat == Oracle.RDFFormat.turtle) {
			RDFDataMgr.write(s, model1, RDFFormat.TURTLE);
			stream.send(s.toString());
			RDFDataMgr.write(s, model2, RDFFormat.TURTLE);
		} else if(rdfFormat == Oracle.RDFFormat.rdfxml) {
			RDFDataMgr.write(s, model1, Lang.RDFXML);
			stream.send(s.toString());
			RDFDataMgr.write(s, model2, Lang.RDFXML);
		} else if(rdfFormat == Oracle.RDFFormat.ntriples) {
			RDFDataMgr.write(s, model1, Lang.NTRIPLES);
			stream.send(s.toString());
			RDFDataMgr.write(s, model2, Lang.NTRIPLES);
		}
		stream.send(s.toString());
		//out.write(s.toString().getBytes(StandardCharsets.UTF_8));

		//logger.info("Number of Triples: " + model2.size());
		/*Iterator<Statement> iterator = model2.listStatements();
		while (iterator.hasNext()) {
			Statement st = iterator.next();
			obsTimestamps.put(st.getSubject().toString(), timestamp);
		}*/
	}

	
	public void importData(int time) throws Exception {
		addTimestampedData(new File("data/data_"+(time < 10 ? "0" + time : time)+".ttl"), time*interval);
	}
	
	public void importAllData() throws Exception {
		for (int i=0;i<=max;i++){
			if (!exclude.contains(""+i)) {
				logger.info("Import" + i);
				importData(i);
				try {
					Thread.sleep(Config.getInstance().getInputStreamInterval());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void setStream(DataStream stream) {
		this.stream = stream;
	}

	public static void setStop(boolean stop) {
		CSRBenchDataStream.stop = stop;
	}

	@Override
	public void run() {
		try {
			importAllData();
		} catch (Exception e) {
			e.printStackTrace();
		}
		stream.stop();
	}
}
