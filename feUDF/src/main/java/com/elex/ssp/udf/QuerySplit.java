package com.elex.ssp.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class QuerySplit extends GenericUDTF {
	
	String[] querys;
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

		querys = args[0].toString().split(args[1].toString());
		for(String word:querys){
			result[0]=word;
			forward(result);
		}
	}

	@Override
	public void close() throws HiveException {

	}


}
