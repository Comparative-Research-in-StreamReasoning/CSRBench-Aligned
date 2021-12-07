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
package eu.planetdata.srbench.oracle.utils.Configuration;

import java.io.File;
import java.io.InputStream;
import java.util.Set;

import eu.planetdata.srbench.oracle.Oracle;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.planetdata.srbench.oracle.io.JsonConverter;
import eu.planetdata.srbench.oracle.query.ContinuousQuery;
import eu.planetdata.srbench.oracle.query.WindowDefinition;
import eu.planetdata.srbench.oracle.result.StreamProcessorOutput;
import eu.planetdata.srbench.oracle.result.StreamProcessorOutputBuilder.R2SOperator;
import eu.planetdata.srbench.oracle.s2r.ReportPolicy;

public class Config {
	private static Config _instance = null;
	private static final Logger logger = LoggerFactory.getLogger(Config.class); 
	
	private Configuration config;
	private JsonConverter converter;

	private String[] queries;
	private Oracle.QueryLanguage queryLanguage = Oracle.QueryLanguage.unsupported;
	private Long timeUnit = 1000L;
	private Long interval = 1000L;
	private Integer maxTime = 33;
	private Set<String> excludes;
	private boolean windowclose;
	private boolean nonemptycontent;
	private boolean contentchange;
	private boolean emptyrelation;
	private R2SOperator r2SOperator;

	
	private Config(){
		converter = new JsonConverter();
		try {
			config = new PropertiesConfiguration("setup.properties");
		} catch (ConfigurationException e) {
			logger.error("Error while reading the configuration file", e);
		}
	}
	
	public File getRepoDir(){
		String dir = config.getString("importer.repo.datadir");
		File ret = new File(dir);
		return ret;
	}

	public String getUnsupportedquery(String key) {
		return config.getString(key+".unsupportedquery");
	}

	public String getUnsupportedqueryWindow(String key) {
		return config.getString(key+".unsupportedqueryWindow");
	}

	public String getRSPQLQuery(String key) {
		return config.getString(key+".rspqlquery");
	}

	public String getCSPARQLQuery(String key) {
		return config.getString(key+".csparqlquery");
	}

	public WindowDefinition getWindowDefinition(String key) {
		return new WindowDefinition(config.getLong(key+".window.size"), config.getLong(key+".window.slide"));
	}
	
	public ContinuousQuery getQuery(String key){
		ContinuousQuery ret = new ContinuousQuery();
		String output = config.getString(key+".output");
		ret.setS2ROperator(r2SOperator);
		ret.setBooleanQuery(config.getString(key+".booleanquery"));
		ret.setRspqlQuery(config.getString(key+".rspqlquery"));
		ret.setWindowDefinition(new WindowDefinition(config.getLong(key+".window.size"), config.getLong(key+".window.slide")));
		ret.setFirstT0(config.getLong(key+".window.firstt0"));
		ret.setStaticData(config.getBoolean(key+".staticdata"));

		String answer = config.getString(key+".answer");
		if(answer!=null){
			InputStream is = getClass().getClassLoader().getResourceAsStream(answer);
			StreamProcessorOutput result = converter.decodeJson(is);
			ret.setAnswer(result);
		}

		return ret;
	}
	
	public String[] getQuerySet(){
		return queries;
	}
	
	public Long getTimeUnit(){
		return timeUnit;
	}
	
	public Integer getInputStreamMaxTime(){
		return maxTime;
	}
	
	public Long getInputStreamInterval(){
		return interval;
	}
	
	public Set<String> getInputStreamHoles(){
		return excludes;
	}

	public ReportPolicy getPolicy(){
		ReportPolicy ret = new ReportPolicy();
		ret.setWindowClose(windowclose);
		ret.setNonEmptyContent(nonemptycontent);
		ret.setContentChange(contentchange);
		return ret;
	}
	
	public boolean getEmtpyRelationOutput(){
		return emptyrelation;
	}
	
	public static Config getInstance(){
		if(_instance==null)
			_instance=new Config();
		return _instance;
	}

	public void setQueries(String[] queries) {
		this.queries = queries;
	}

	public void setQueryLanguage(Oracle.QueryLanguage queryLanguage) {
		this.queryLanguage = queryLanguage;
	}

	public void setTimeUnit(Long timeUnit) {
		this.timeUnit = timeUnit;
	}

	public void setInterval(Long interval) {
		this.interval = interval;
	}

	public void setMaxTime(Integer maxTime) {
		this.maxTime = maxTime;
	}

	public void setExcludes(Set<String> excludes) {
		this.excludes = excludes;
	}

	public void setWindowclose(boolean windowclose) {
		this.windowclose = windowclose;
	}

	public void setNonemptycontent(boolean nonemptycontent) {
		this.nonemptycontent = nonemptycontent;
	}

	public void setContentchange(boolean contentchange) {
		this.contentchange = contentchange;
	}

	public void setEmptyrelation(boolean emptyrelation) {
		this.emptyrelation = emptyrelation;
	}

	public void setR2SOperator(R2SOperator r2SOperator) {
		this.r2SOperator = r2SOperator;
	}

	public Oracle.QueryLanguage getQueryLanguage() {
		return queryLanguage;
	}

	public R2SOperator getR2SOperator() {
		return r2SOperator;
	}
}
