 package parquet.cascading;

 import java.io.IOException;

 import org.apache.hadoop.mapred.JobConf;
 import org.apache.hadoop.mapred.OutputCollector;
 import org.apache.hadoop.mapred.RecordReader;

 import parquet.example.data.simple.SimpleGroup;
 import parquet.hadoop.example.ExampleInputFormat;

 import cascading.flow.FlowProcess;
 import cascading.scheme.SinkCall;
 import cascading.scheme.Scheme;
 import cascading.scheme.SourceCall;
 import cascading.tap.Tap;
 import cascading.tuple.Tuple;
 import cascading.tuple.Fields;


public class ParquetTupleScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>{

  private static final long serialVersionUID = 0L;

  public ParquetTupleScheme(Fields sourceFields) {
    super(sourceFields);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    jobConf.setInputFormat(DeprecatedExampleInputFormat.class);
 }

  @SuppressWarnings("unchecked")
  @Override
  public boolean source(FlowProcess<JobConf> fp, SourceCall<Object[], RecordReader> sc)
      throws IOException {
    Container<SimpleGroup> value = (Container<SimpleGroup>) sc.getInput().createValue();
    boolean hasNext = sc.getInput().next(null, value);
    if (!hasNext) { return false; }

    // Skip nulls
    if (value == null) { return true; }

    SimpleGroup group = value.get();
    try {
      Tuple tuple = new Tuple();
      for(Comparable f : getSourceFields()) {
        String fieldName = f.toString();
        int index = group.getType().getFieldIndex(fieldName);
        tuple.add(group.getValueToString(index, 0));
      }
      sc.getIncomingEntry().setTuple(tuple);
    } catch (Exception e) {
      System.out.println("Caught " + e + " when processing " + group);
    }
    return true;
  }


  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> arg0,
      Tap<JobConf, RecordReader, OutputCollector> arg1, JobConf arg2) {
    throw new UnsupportedOperationException("ParquetTupleScheme does not support Sinks");

  }

  @Override
  public boolean isSink() { return false; }


  @Override
  public void sink(FlowProcess<JobConf> arg0, SinkCall<Object[], OutputCollector> arg1)
      throws IOException {
    throw new UnsupportedOperationException("ParquetTupleScheme does not support Sinks");
  }
}