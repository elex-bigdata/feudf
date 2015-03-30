package com.elex.ssp.udf;

import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.elex.ssp.TimeUtils;

public class TimeDim extends GenericUDTF {
	
	Object hour[] = new Object[1];
	Object workOrVaction[] = new Object[1];
	Object dayPart[] = new Object[1];
	
	@Override
	public void close() throws HiveException {

	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)throws UDFArgumentException {
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("col1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		if(args.length==2){
			if(args[0]!=null && args[1]!=null){
				String[] myArgs = { args[0].toString(), args[1].toString() };
				
					Date day = TimeUtils.getTimeByNation(myArgs);
					hour[0] = TimeUtils.getHour(day);
					dayPart[0] = TimeUtils.getDayPart(day);
					workOrVaction[0] = TimeUtils.isWorkOrVacation(day);
					forward(hour);
					forward(dayPart);
					forward(workOrVaction);

				
			}
			
		}
		
		
		
	}

}
