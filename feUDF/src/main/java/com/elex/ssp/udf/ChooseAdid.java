package com.elex.ssp.udf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * GenericUDAFCollectSet
 */
@Description(name = "chooseAdid", value = "_FUNC_(x) - Returns a set of objects with duplicate elements eliminated")
public class ChooseAdid extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(ChooseAdid.class.getName());
  
  public ChooseAdid() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    //判别参数个数
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }
    //判别是否是基本类型，可以重写成支持复合类型
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    //指定调用的Evaluator,用来接收消息和指定UDAF如何调用
    return new GenericUDAFMkSetEvaluator();
  }

  public static class GenericUDAFMkSetEvaluator extends GenericUDAFEvaluator {
    
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private PrimitiveObjectInspector inputOI;
    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
    // of objs)
    private StandardListObjectInspector loi;
    
    private StandardListObjectInspector internalMergeOI;
    
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      // init output object inspectors
      // The output of a partial aggregation is a list
      /**
      * collect_set函数每个阶段分析
      * 1.PARTIAL1阶段，原始数据到部分聚合，在collect_set中，则是将原始数据放入set中，所以，
      * 输入数据类型是PrimitiveObjectInspector，输出类型是StandardListObjectInspector
      * 2.在其他情况，有两种情形：（1）两个set之间的数据合并，也就是不满足if条件情况下
      *（2）直接从原始数据到set，这种情况的出现是为了兼容从原始数据直接到set，也就是说map后
      * 直接到输出，没有reduce过程，也就是COMPLETE阶段
      */
      if (m == Mode.PARTIAL1) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        return ObjectInspectorFactory
            .getStandardListObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
                .getStandardObjectInspector(inputOI));
      } else {
        //COMPLETE 阶段
        if (!(parameters[0] instanceof StandardListObjectInspector)) {
          //no map aggregation.
          inputOI = (PrimitiveObjectInspector)  ObjectInspectorUtils
          .getStandardObjectInspector(parameters[0]);
          return (StandardListObjectInspector) ObjectInspectorFactory
              .getStandardListObjectInspector(inputOI);
        } else { //PARTIAL2,FINAL阶段，两个阶段都是list与list合并，调用一致
          internalMergeOI = (StandardListObjectInspector) parameters[0];
          inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
          loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);          
          return loi;
        }
      }
    }
    
    static class MkArrayAggregationBuffer implements AggregationBuffer {
      Set<Object> container;
    }
    
    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((MkArrayAggregationBuffer) agg).container = new HashSet<Object>();
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
      reset(ret);
      return ret;
    }

    //mapside，将原始值转换添加到集合中
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      Object p = parameters[0];

      if (p != null) {
        MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
        putIntoSet(p, myagg);
      }
    }

    //mapside，临时聚集
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
      ret.addAll(myagg.container);
      return ret;
    }
    //terminatePartial的临时聚集跟另一个聚集合并
    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
      for(Object i : partialResult) {
        putIntoSet(i, myagg);
      }
    }
    
    //合并最终结果到结果集返回
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
      ret.addAll(myagg.container);
      return ret;
    }
    
    private void putIntoSet(Object p, MkArrayAggregationBuffer myagg) {
      if (myagg.container.contains(p))
        return;
      Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,
          this.inputOI);
      myagg.container.add(pCopy);
    }
  }
  
}
