/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.elex.ssp.udf;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * This is a simple UDAF that concatenates all arguments from different rows
 * into a single string.
 * 
 * It should be very easy to follow and can be used as an example for writing
 * new UDAFs.
 * 
 * Note that Hive internally uses a different mechanism (called GenericUDAF) to
 * implement built-in aggregation functions, which are harder to program but
 * more efficient.
 */

public class ChooseDt extends UDAF {

	/**
	 * The actual class for doing the aggregation. Hive will automatically look
	 * for all internal classes of the UDAF that implements UDAFEvaluator.
	 */
	public static class UDAFExampleGroupConcatEvaluator implements
			UDAFEvaluator {

		private ArrayList<String> data;
		//private String split;
		private String pb;

		public UDAFExampleGroupConcatEvaluator() {
			super();
			data = new ArrayList<String>();
			pb = null;
		}

		/**
		 * Reset the state of the aggregation.
		 */
		public void init() {
			data.clear();
		}

		/**
		 * Iterate through one row of original data.
		 * 
		 * This UDF accepts arbitrary number of String arguments, so we use
		 * String[]. If it only accepts a single String, then we should use a
		 * single String argument.
		 * 
		 * This function should always return true.
		 */
		public boolean iterate(String o, String sed, String dt) {
			//split = sed;
			if (o != null) {
				if (o.trim().equals(dt.trim())) {
					pb = dt;
					return true;
				}else if (!o.trim().equals(dt)) {
					data.add(o + sed);
				}

			}
			return true;
		}

		/**
		 * Terminate a partial aggregation and return the state.
		 */
		public ArrayList<String> terminatePartial() {
			return data;
		}

		/**
		 * Merge with a partial aggregation.
		 * 
		 * This function should always have a single argument which has the same
		 * type as the return value of terminatePartial().
		 * 
		 * This function should always return true.
		 */
		public boolean merge(ArrayList<String> o) {
			if (o != null) {
				data.addAll(o);
			}
			return true;
		}

		/**
		 * Terminates the aggregation and return the final result.
		 */
		public String terminate() {
			// 以下代码块实现按传入的sed参数concat参数o，并对o去重并去掉pbID然后输出。
			/*
			 * StringBuilder sb = new StringBuilder(); Set<String> ret = new
			 * HashSet<String>(); ret.addAll(data); Iterator<String> ite =
			 * ret.iterator();
			 * 
			 * while(ite.hasNext()){ sb.append(ite.next()); }
			 * 
			 * if(split != null){ if(!split.equals("")){ return
			 * sb.toString().substring(0,sb.toString().lastIndexOf(split)); } }
			 * 
			 * return sb.toString();
			 */
			Collections.sort(data);
			if(pb != null){
				return pb;
			}else if (data != null) {
				if (data.size() > 0) {
					return data.get(0);
				}
			}
			return null;
		}

	}

}