package com.elex.ssp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.elex.ssp.QueryUtils;

public class QueryLength extends UDF {

	public String evaluate(String query) {

		if (query == null) {
			return "0";

		} else {
			if (QueryUtils.normalize(query) == null) {
				return "0";
			} else {
				if (QueryUtils.normalize(query).equals(" ")) {
					return "0";
				} else {
					return Integer.toString(QueryUtils.getQueryLength(query,false));
				}
			}
		}

	}

}
