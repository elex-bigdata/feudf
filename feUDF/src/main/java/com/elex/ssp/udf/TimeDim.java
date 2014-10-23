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
	
	Object hour;
	Object workOrVaction;
	Object dayPart;

	 
	  @Override
	  public void close() throws HiveException {
		  hour=null;
		  workOrVaction=null;
		  dayPart=null;
	  }
	 
	  @Override
	  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
	    ArrayList<String> fieldNames = new ArrayList<String>();
	    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
	    fieldNames.add("col1");
	    fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
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
			forward(hour);
			forward(dayPart);
			forward(workOrVaction);
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
	  }

}
