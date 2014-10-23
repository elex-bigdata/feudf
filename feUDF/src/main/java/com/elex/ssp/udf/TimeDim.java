package com.elex.ssp.udf;

import java.text.ParseException;
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
	
	String hour;
	String workOrVaction;
	String dayPart;

	 
	  @Override
	  public void close() throws HiveException {

	  }
	 
	  @Override
	  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
	    ArrayList<String> fieldNames = new ArrayList<String>();
	    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	    fieldNames.add("col1");
	    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
	fieldOIs);
	  }
	 
	  @Override
	  public void process(Object[] args) throws HiveException {
		  String[] myArgs = {args[0].toString(),args[1].toString()};
		 try {
			Date day = TimeUtils.getTimeByNation(myArgs);
			hour = TimeUtils.getHour(day);
			dayPart = TimeUtils.getDayPart(day);
			workOrVaction = TimeUtils.isWorkOrVacation(day);
			forward((Object)hour);
			forward((Object)dayPart);
			forward((Object)workOrVaction);
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
	  }

}
