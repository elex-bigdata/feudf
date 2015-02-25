package com.elex.ssp.udf;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.elex.ssp.QueryUtils;
import com.elex.ssp.WordSeder;

public class Sed extends UDF {

	public String evaluate(String query) {
		List<String> keywords;
		StringBuffer result = null;
		try {
			if(query==null){
				return null;
			}else{
				if(QueryUtils.normalize(query.toString())==null){
					return null;
				}else{
					keywords = WordSeder.sed(QueryUtils.normalize(query.toString()));
					result = new StringBuffer(100);
					for(String word:keywords){
						result.append(word+" ");
					}
					return result.toString();
				}
			}
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
     }
}
