package com.elex.ssp.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class PairID extends GenericUDTF {
	
	String[] adids;
	String[] camp_ids;
	String result[] = new String[2];

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		ArrayList<String> fieldNames = new ArrayList<String>();
	    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	    fieldNames.add("col1");
	    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    fieldNames.add("col2");
	    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
	        fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {

		if(args.length != 2){
			
		}else{
			if(args[0]==null || args[1] == null){
				
			}else{
				adids = args[0].toString().split(",");
				camp_ids = args[0].toString().split(",");
				
				for(int i =0;i<Math.min(adids.length, camp_ids.length);i++){
					result[0]=adids[i];
					result[1]=camp_ids[i];
					forward(result);
				}
			}				
		}
	}

	@Override
	public void close() throws HiveException {
		
	}


}
