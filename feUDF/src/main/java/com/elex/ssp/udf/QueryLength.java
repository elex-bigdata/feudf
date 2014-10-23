package com.elex.ssp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.elex.ssp.QueryUtils;

public class QueryLength extends UDF {

	public String evaluate(String query) {
		
        return Integer.toString(QueryUtils.getQueryLength(query, false));
        
        }


}
