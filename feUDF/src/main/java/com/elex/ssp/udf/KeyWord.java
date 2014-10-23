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

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		ArrayList<String> fieldNames = new ArrayList<String>();
	    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	    fieldNames.add("col1");
	    fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
	        fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {

		try {
			keywords = WordSeder.sed(QueryUtils.normalize(args[0].toString()));
			for(Object word:keywords){
				forward(word);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws HiveException {
		keywords.clear();
	}


}
