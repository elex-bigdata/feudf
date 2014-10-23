package com.elex.ssp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.elex.ssp.QueryUtils;

public class WordCount extends UDF {

	public String evaluate(String query) {
		
        return Integer.toString(QueryUtils.getQueryWordCount(query, false));
        
        }


}
