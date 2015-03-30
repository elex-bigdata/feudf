package com.elex.ssp.udf;

import java.util.Date;
import org.apache.hadoop.hive.ql.exec.UDF;
import com.elex.ssp.TimeUtils;

public class DayPart extends UDF {

	public String evaluate(String time, String nation) {
		String[] myArgs = { time.toLowerCase(), nation.toLowerCase() };
		Date day = TimeUtils.getTimeByNation(myArgs);
		return TimeUtils.getDayPart(day);
	}

}
