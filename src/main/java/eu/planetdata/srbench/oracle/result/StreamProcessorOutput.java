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
package eu.planetdata.srbench.oracle.result;

import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamProcessorOutput {
	private static final Logger logger = LoggerFactory.getLogger(StreamProcessorOutput.class);

	private List<TimestampedRelation> results;

	public StreamProcessorOutput() {
		results = new ArrayList<TimestampedRelation>();
	}

	public List<TimestampedRelation> getResultRelations() {
		return results;
	}

	public void addRelation(TimestampedRelation relation){
		results.add(relation);
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		for(TimestampedRelation rel: results)
			for(TimestampedRelationElement srr : rel.getBindings())
				ret.append(srr.toString());
		return ret.toString();
	}

	public boolean contains(StreamProcessorOutput outputStream){
		List<TimestampedRelation> subSeq = groupIt(outputStream.getResultRelations());
//		List<TimestampedRelation> subSeq = outputStream.getResultRelations();
		int totalResultSum = results.stream().mapToInt(t -> t.results.size()).sum();
		results = groupIt(results);
		//logger.info("Sizes: " + totalResultSum + " " + subSeq.size());
		if(results.size()<subSeq.size()){
			logger.debug("the subset size is greater than the set size!");
			final List<TimestampedRelation> list = new ArrayList<>(subSeq);
			List<String> list2 = results.stream().map(c -> c.toString()).collect(Collectors.toList());
			list.removeIf(c -> list2.contains(c.toString()));
			return false;
		}
		if(subSeq.size()==0)
			return true;
		TimestampedRelation firstResult = subSeq.get(0);
		for(int i=0; i<results.size(); i++){
			logger.debug("Comparing {} with {}", results.get(i), firstResult);
			if(results.get(i).equals(firstResult)){
				boolean exit=false;
				int j=i+1;
				for(int k=1; k<subSeq.size() && exit==false;){
					if((j>=results.size() && k<subSeq.size()) || !results.get(j++).equals(subSeq.get(k++)))
						exit=true;
				}
				if(!exit)
					return true;
			}
		}
		return false;
	}

	public List<TimestampedRelation> groupIt(List<TimestampedRelation> list) {
		Map<Long, TimestampedRelation> map = new HashMap<>();
		for(TimestampedRelation tr : list) {
			for(TimestampedRelationElement tre : tr.results) {
				if(map.containsKey(tre.getTimestamp())) {
					map.get(tre.getTimestamp()).addElement(tre);
				}
				else {
					TimestampedRelation tr2 = new TimestampedRelation();
					tr2.addElement(tre);
					map.put(tre.getTimestamp(), tr2);
				}
			}
		}
		System.out.println("A: " + map.values());
		List<TimestampedRelation> list2 = new ArrayList<>(map.values());
		list2.sort(Comparator.comparingLong(a -> a.results.iterator().next().getTimestamp()));
		return list2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result;
		result = prime * result + ((results == null) ? 0 : results.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StreamProcessorOutput other = (StreamProcessorOutput) obj;
		if (results == null) {
			if (other.results != null)
				return false;
		} else if (!results.equals(other.results))
			return false;
		return true;
	}


}
