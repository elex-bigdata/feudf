package com.elex.ssp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.elex.ssp.QueryUtils;

public class Query extends UDF {

	public String evaluate(String query) {
		
        return QueryUtils.normalize(query);
        
        }


}
