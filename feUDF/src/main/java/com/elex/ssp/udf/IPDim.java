package com.elex.ssp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class IPDim extends UDF {

	public String evaluate(String ip) {
        return getArea(ip);
        }
	
	public static String getArea(String ip){
		String area = null;

		String[] ip_seg;
		if(ip != null){
			if(!ip.trim().equals("")){
				ip_seg=ip.split("\\.");
				if(ip_seg.length>=2){
					area = ip_seg[0]+"."+ip_seg[1];
				}else{
					area=ip;
				}
			}			
		}
		
		return area;
	}


}
