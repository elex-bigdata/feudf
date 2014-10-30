package com.elex.ssp.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.elex.ssp.WordSeder;
import com.elex.ssp.QueryUtils;

public class KeyWord extends GenericUDTF {
	
	List<String> keywords;
	Object result[] = new Object[1];

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		ArrayList<String> fieldNames = new ArrayList<String>();
	    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	    fieldNames.add("col1");
	    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
	        fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {

		try {
			if(args.length != 1){
				
			}else{
				if(args[0]==null){
					
				}else{
					if(QueryUtils.normalize(args[0].toString())==null){
						
					}else{
						keywords = WordSeder.sed(QueryUtils.normalize(args[0].toString()));
						for(String word:keywords){
							result[0]=word;
							forward(result);
						}
					}
				}				
			}
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws HiveException {

	}


}
