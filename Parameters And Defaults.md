# About Parameters Of Aligned Benchmarks And Their Defaults

**Required Parameters:**
 * Benchmark Parameters (i.e. all parameters that are necessary for running the actual benchmark)
 * _engineName_ = (String)e, specify engine name for publishing of results

**Optional Parameters:**
 * _queryLanguage_ = (String)"rspql" or "csparql" or "unsupported", (default: unsupported), query language that is used for providing the configuration. If query language is supported, just the necessary queries are provided in the configurations, otherwise serialized [Query](https://github.com/SRrepo/CSRBench-Aligned/blob/master/src/main/java/eu/planetdata/srbench/oracle/utils/QueryInformation/Query.java)-objects are provided (in JSON format).
 * _rdfFormat_ = (String)"ntriples" or "turtle" or "rdfxml", (default: turtle), specifies the transmission format for the RDF data stream
 * _waitingTime_ = (Integer)w, (default: 3000), specifies how long the benchmark waits for the SR engine to write its last answers (in ms)
 * _configPath_ = (String)c, (default: "http://localhost:11111/configuration.json"), specifies where the benchmark publishes the configuration file
 * _answerPath_ = (String)a, (default: "http://localhost:11112/answers.json"), specifies where the benchmark expects the answers of the engine
 * _resultsPath_ = (String)r, (default: "http://localhost:11113/results.html"), specifies where the benchmark publishes the results of the experiment
