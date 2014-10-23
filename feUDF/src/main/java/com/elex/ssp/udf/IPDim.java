package com.elex.ssp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class IPDim extends UDF {

	public String evaluate(String ip) {
        return getArea(ip);
        }
	
	public static String getArea(String ip){
		String area = null;
		
		if(ip != null){
			if(!ip.trim().equals("")){
				if(ip.split("\\.").length==4){
					area = ip.substring(0, ip.lastIndexOf("."));
				}else{
					area=ip;
				}
			}			
		}
		
		return area;
	}


}
