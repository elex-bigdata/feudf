package com.elex.ssp.udf;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.elex.ssp.URLUtil;

public class Domain extends UDF {

	public String evaluate(String url) {
		return getDomain(url);
	}

	public static String getDomain(String url) {
		String domain = null;

		if (url != null) {
			url = url.replace(",", "");
			try {
				if (url.startsWith("http") || url.startsWith("https")) {
					domain = URLUtil.getDomainName(new URL(url));
				} else {
					domain = URLUtil.getDomainName(new URL("http://" + url));
				}
			} catch (MalformedURLException e) {
				System.out.println("can't parse:"+url);
			}
		}

		return domain;
	}

}
