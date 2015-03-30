package com.elex.ssp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.elex.ssp.GeoIpCity;

public class City extends UDF {

	public String evaluate(String ip) {
		return GeoIpCity.getCityName(ip);
	}

}
