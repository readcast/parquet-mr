/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.cascading;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.twitter.elephantbird.util.HadoopUtils;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetRecordReader;

@SuppressWarnings("deprecation")
public abstract class DeprecatedParquetInputFormat<V> implements org.apache.hadoop.mapred.InputFormat<Container<Void>, Container<V>> {

  protected ParquetInputFormat<V> realInputFormat;

  @Override
  public RecordReader<Container<Void>, Container<V>> getRecordReader(InputSplit split, JobConf job,
                  Reporter reporter) throws IOException {
    return new RecordReaderWrapper<V>(realInputFormat, split, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      List<org.apache.hadoop.mapreduce.InputSplit> splits =
        realInputFormat.getSplits(job);

      if (splits == null) {
        return null;
      }

      InputSplit[] resultSplits = new InputSplit[splits.size()];
      int i = 0;
      for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
        if (split.getClass() == org.apache.hadoop.mapreduce.lib.input.FileSplit.class) {
          org.apache.hadoop.mapreduce.lib.input.FileSplit mapreduceFileSplit =
              ((org.apache.hadoop.mapreduce.lib.input.FileSplit)split);
          resultSplits[i++] = new FileSplit(
              mapreduceFileSplit.getPath(),
              mapreduceFileSplit.getStart(),
              mapreduceFileSplit.getLength(),
              mapreduceFileSplit.getLocations());
        } else {
          resultSplits[i++] = new InputSplitWrapper(split);
        }
      }

      return resultSplits;
  }

  /**
   * A reporter that works with both mapred and mapreduce APIs.
   */
  private static class ReporterWrapper extends StatusReporter implements Reporter {
    private Reporter wrappedReporter;

    public ReporterWrapper(Reporter reporter) {
      wrappedReporter = reporter;
    }

    @Override
    public Counters.Counter getCounter(Enum<?> anEnum) {
      return wrappedReporter.getCounter(anEnum);
    }

    @Override
    public Counters.Counter getCounter(String s, String s1) {
      return wrappedReporter.getCounter(s, s1);
    }

    @Override
    public void incrCounter(Enum<?> anEnum, long l) {
      wrappedReporter.incrCounter(anEnum, l);
    }

    @Override
    public void incrCounter(String s, String s1, long l) {
      wrappedReporter.incrCounter(s, s1, l);
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return wrappedReporter.getInputSplit();
    }

    @Override
    public void progress() {
      wrappedReporter.progress();
    }

    @Override
    public void setStatus(String s) {
      wrappedReporter.setStatus(s);
    }
  }

  private static class RecordReaderWrapper<V> implements RecordReader<Container<Void>, Container<V>> {

    private ParquetRecordReader<V> realReader;
    private long splitLen; // for getPos()

    // expect readReader return same Key & Value objects (common case)
    // this avoids extra serialization & deserialazion of these objects
    private Container<Void> keyContainer = null;
    private Container<V> valueContainer = null;

    private boolean firstRecord = false;
    private boolean eof = false;

    public RecordReaderWrapper(ParquetInputFormat<V> newInputFormat,
                               InputSplit oldSplit,
                               JobConf oldJobConf,
                               Reporter reporter) throws IOException {

      splitLen = oldSplit.getLength();

      org.apache.hadoop.mapreduce.InputSplit split;
      if (oldSplit.getClass() == FileSplit.class) {
        split = new org.apache.hadoop.mapreduce.lib.input.FileSplit(
            ((FileSplit)oldSplit).getPath(),
            ((FileSplit)oldSplit).getStart(),
            ((FileSplit)oldSplit).getLength(),
            oldSplit.getLocations());
      } else {
        split = ((InputSplitWrapper)oldSplit).realSplit;
      }

      TaskAttemptID taskAttemptID = TaskAttemptID.forName(oldJobConf.get("mapred.task.id"));
      if (taskAttemptID == null) {
        taskAttemptID = new TaskAttemptID();
      }

      try {
        realReader = new ParquetRecordReader<V>(newInputFormat.getReadSupport(oldJobConf));
        realReader.initialize(split, oldJobConf);

        // read once to gain access to key and value objects
        if (realReader.nextKeyValue()) {
          firstRecord = true;
          keyContainer = new Container<Void>();
          keyContainer.set(realReader.getCurrentKey());
          valueContainer = new Container<V>();
          valueContainer.set(realReader.getCurrentValue());

        } else {
          eof = true;
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      realReader.close();
    }

    @Override
    public Container<Void> createKey() {
      return keyContainer;
    }

    @Override
    public Container<V> createValue() {
      return valueContainer;
    }

    @Override
    public long getPos() throws IOException {
      return (long) (splitLen * getProgress());
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return realReader.getProgress();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public boolean next(Container<Void> key, Container<V> value) throws IOException {
      if (eof) {
        return false;
      }

      if (firstRecord) { // key & value are already read.
        firstRecord = false;
        return true;
      }

      try {
        if (realReader.nextKeyValue()) {
          if (key != null) key.set(realReader.getCurrentKey());
          if (value != null) value.set(realReader.getCurrentValue());
          return true;
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }

      eof = true; // strictly not required, just for consistency
      return false;
    }
  }



  private static class InputSplitWrapper implements InputSplit {

    org.apache.hadoop.mapreduce.InputSplit realSplit;


    @SuppressWarnings("unused") // MapReduce instantiates this.
    public InputSplitWrapper() {}

    public InputSplitWrapper(org.apache.hadoop.mapreduce.InputSplit realSplit) {
      this.realSplit = realSplit;
    }

    @Override
    public long getLength() throws IOException {
      try {
        return realSplit.getLength();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public String[] getLocations() throws IOException {
      try {
        return realSplit.getLocations();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String className = WritableUtils.readString(in);
      Class<?> splitClass;

      try {
        splitClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      realSplit = (org.apache.hadoop.mapreduce.InputSplit)
                  ReflectionUtils.newInstance(splitClass, null);
      ((Writable)realSplit).readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, realSplit.getClass().getName());
      ((Writable)realSplit).write(out);
    }
  }
}
