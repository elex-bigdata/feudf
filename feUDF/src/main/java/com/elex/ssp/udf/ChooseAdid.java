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

import com.elex.ssp.PropertiesUtils;

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

public class ChooseAdid extends UDAF {
	
	static class UDAFState {
		private ArrayList<String> data = new ArrayList<String>();
		private String origId;
	}

  /**
   * The actual class for doing the aggregation. Hive will automatically look
   * for all internal classes of the UDAF that implements UDAFEvaluator.
   */
  public static class UDAFExampleGroupConcatEvaluator implements UDAFEvaluator {

	  String[] passBackIds;
	  UDAFState state;

    public UDAFExampleGroupConcatEvaluator() {
      super();
      state = new UDAFState();
      init();
    }

    /**
     * Reset the state of the aggregation.
     */
    public void init() {
    	state.data.clear();
    	passBackIds = PropertiesUtils.getPbIDs();
    }

    /**
     * Iterate through one row of original data.
     * 
     * This UDF accepts arbitrary number of String arguments, so we use
     * String[]. If it only accepts a single String, then we should use a single
     * String argument.
     * 
     * This function should always return true.
     */
    public boolean iterate(String o) {
    	state.origId=o;
      if (o != null) {
    	  for(String pbid:passBackIds){
    		  if(!o.trim().equals(pbid)){
        		  state.data.add(o);
        	  } 
    	  }
    	  
    	       
      }
      return true;
    }

    /**
     * Terminate a partial aggregation and return the state.
     */
    public UDAFState terminatePartial() {
      return state;
    }

    /**
     * Merge with a partial aggregation.
     * 
     * This function should always have a single argument which has the same
     * type as the return value of terminatePartial().
     * 
     * This function should always return true.
     */
    public boolean merge(UDAFState o) {
      if (o != null) {
        state.data.addAll(o.data);
        state.origId=o.origId;
      }
      return true;
    }

    /**
     * Terminates the aggregation and return the final result.
     */
    public String terminate() {
    	Collections.sort(state.data);
    	if( state.data != null){
    		if( state.data.size()>0){
    			return  state.data.get(0);
    		}else{
    			return  state.origId;
    		}   		
    	}
    	return state.origId;
    }
    
  }
  
  

}