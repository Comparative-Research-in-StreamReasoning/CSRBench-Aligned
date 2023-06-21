# Paper
This work is part of the paper **"A Comparative Study of Stream Reasoning Engines"**. You can find the full paper at [https://doi.org/10.1007/978-3-031-33455-9_2](https://doi.org/10.1007/978-3-031-33455-9_2).

# Aligned CSRBench-oracle

**Requirements:**
 * Java 7+
 * Maven

**License:**
 * The code is released under the Apache 2.0 license

**How to run**
1. Run eu.planetdata.srbench.oracle.Oracle
2. Run a project, that implements the unifying interface and reads the data from the given configURL (see https://github.com/Comparative-Research-in-StreamReasoning/CSPARQL-Example-Runner-Unifying-Interface)

**The project has one main file:**
* (eu.planetdata.srbench.oracle.)**Oracle**:
  1. The oracle initializes its repository and parses the input parameters
  2. The oracle publishes a configuration-file at a specified URL (default: http://localhost:11111/configuration.json) as well as optional static data sources, and opens socket connections for every stream
  3. The oracle starts streaming the RDF data in the configured format when a client is connected to every socket
  4. The engine is expected to publish its answers under a specified URL (default: http://localhost:11112/answers.json)
  5. The oracle loads the answers (as json-serialized eu.planetdata.srbench.oracle.io.answers.**TransferObject**) after waiting a specified interval of time (default: 3000ms) and evaluates them against expected answers with different starting points t<sub>0</sub>

**Configuration:**
 * The oracle can be configured by using the following commandline parameters
   * _queries_ = (String)q, (required) //queries to be executed seperated with commas (e.g., srb.query1)
   * _windowclose_ = (boolean)wc, (required) //configure whether engine uses windowclose report strategy
   * _contentchange_ = (boolean)cc, (required) //configure whether engine uses contentchange report strategy
   * _nonemptycontent_ = (boolean)ne, (required) //configure whether engine uses nonemptycontent report strategy
   * _emptyrelation_ = (boolean)er, (required) //configure whether engine reports empty results
   * _r2SOperator_ = (String)"RStream" or "IStream" or "DStream", (required) //configure R2S operator of engine 
   * _engineName_ = (String)e (required), specify engine name for publishing of results
   * _queryLanguage_ = (String)"rspql" or "csparql" or "unsupported", (optional) (default: unsupported) //query language that is used for providing the configuration. If query language is supported just the necessary queries are provided in the configurations, otherwise serialized query-objects are provided.
   * _rdfFormat_ = (String)"ntriples" or "turtle" or "rdfxml", (optional) (default: turtle) //specifies the transmission format for the RDF data stream
   * _waitingTime_ = (Integer)w, (optional) (default: 3000) //specifies how long the benchmark, waits for the SR engine to write its last answers
   * _detailedResults_ = (boolean)d, (optional) (default: false) //specifies whether detailed results are given after the experiment
   * _configPath_ = (String)c, (optional) (default: "http://localhost:11111/configuration.json") //specifies where the benchmark publishes the configuration
   * _answerPath_ = (String)a, (optional) (default: "http://localhost:11112/answers.json") //specifies where the benchmark expects the answers of the engine
   * _resultsPath_ = (String)r, (optional) (default: "http://localhost:11113/results.html") //specifies where the benchmark publishes the results of the experiment
   * _excludes_ = (String)e, (optional) (default: "") //specifies which data-files are excluded
   * _maxTime_ = (Integer)m, (optional) (default: 33) //specifies the last data-files to be streamed
   * _interval_ = (Long)i, (optional) (default: 1000) //specifies the time interval between the streaming of RDF graphs
   * _timeUnit_ = (Long)t, (optional) (default: 1000) //specifies the internal time unit of the oracle

**Example configuration:**
queries=srb.query1 queryLanguage=csparql windowclose=true nonemptycontent=true contentchange=false emptyrelation=true r2SOperator=RStream engineName=csparql rdfFormat=ntriples waitingTime=5000 detailedResults=true


