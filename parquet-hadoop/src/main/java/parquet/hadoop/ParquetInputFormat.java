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
package parquet.hadoop;

import static parquet.hadoop.ParquetFileWriter.mergeInto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ReflectionUtils;

import parquet.Log;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 * The input format to read a Parquet file.
 *
 * It requires an implementation of {@link ReadSupport} to materialize the records.
 *
 * The requestedSchema will control how the original records get projected by the loader.
 * It must be a subset of the original schema. Only the columns needed to reconstruct the records with the requestedSchema will be scanned.
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class ParquetInputFormat<T> extends FileInputFormat<Void, T> {

  private static final Log LOG = Log.getLog(ParquetInputFormat.class);

  public static final String READ_SUPPORT_CLASS = "parquet.read.support.class";

  public static void setReadSupportClass(Job job,  Class<?> readSupportClass) {
    job.getConfiguration().set(READ_SUPPORT_CLASS, readSupportClass.getName());
  }

  public static void setReadSupportClass(JobConf conf, Class<?> readSupportClass) {
    conf.set(READ_SUPPORT_CLASS, readSupportClass.getName());
  }

  public static Class<?> getReadSupportClass(Configuration configuration) {
    final String className = configuration.get(READ_SUPPORT_CLASS);
    if (className == null) {
      return null;
    }
    try {
      final Class<?> readSupportClass = Class.forName(className);
      if (!ReadSupport.class.isAssignableFrom(readSupportClass)) {
        throw new BadConfigurationException("class " + className + " set in job conf at " + READ_SUPPORT_CLASS + " is not a subclass of ReadSupport");
      }
      return readSupportClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("could not instanciate class " + className + " set in job conf at " + READ_SUPPORT_CLASS , e);
    }
  }

  private Class<?> readSupportClass;

  private List<Footer> footers;

  /**
   * Hadoop will instantiate using this constructor
   */
  public ParquetInputFormat() {
  }

  /**
   * constructor used when this InputFormat in wrapped in another one (In Pig for example)
   * @param readSupportClass the class to materialize records
   */
  public <S extends ReadSupport<T>> ParquetInputFormat(Class<S> readSupportClass) {
    this.readSupportClass = readSupportClass;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<Void, T> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new ParquetRecordReader<T>(getReadSupport(taskAttemptContext.getConfiguration()));
  }

  public ReadSupport<T> getReadSupport(Configuration configuration){
    try {
      if (readSupportClass == null) {
        readSupportClass = getReadSupportClass(configuration);
      }
      @SuppressWarnings("unchecked") // I know
      ReadSupport<T> readSupport = (ReadSupport<T>)readSupportClass.newInstance();
      return readSupport;
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instanciate read support class", e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instanciate read support class", e);
    }
  }

  private String getRequestedSchema(String fileSchema, String requestedSchema) {
    if (requestedSchema != null) {
      MessageType requestedMessageType = MessageTypeParser.parseMessageType(requestedSchema);
      MessageType fileMessageType = MessageTypeParser.parseMessageType(fileSchema);
      fileMessageType.checkContains(requestedMessageType);
      return requestedSchema;
    }
    return fileSchema;
  }

  /**
   * groups together all the data blocks for the same HDFS block
   * @param blocks data blocks (row groups)
   * @param hdfsBlocks hdfs blocks
   * @param fileStatus the containing file
   * @param fileMetaData file level meta data
   * @param extraMetadata
   * @param readSupport how to materialize the records
   * @return the splits (one per HDFS block)
   * @throws IOException If hosts can't be retrieved for the HDFS block
   */
  static <T> List<InputSplit> generateSplits(List<BlockMetaData> blocks,
      BlockLocation[] hdfsBlocks, FileStatus fileStatus,
      FileMetaData fileMetaData, Class<?> readSupportClass, String requestedSchema) throws IOException {
    Comparator<BlockLocation> comparator = new Comparator<BlockLocation>() {
      @Override
      public int compare(BlockLocation b1, BlockLocation b2) {
        return Long.signum(b1.getOffset() - b2.getOffset());
      }
    };
    Arrays.sort(hdfsBlocks, comparator);
    List<List<BlockMetaData>> splitGroups = new ArrayList<List<BlockMetaData>>(hdfsBlocks.length);
    for (int i = 0; i < hdfsBlocks.length; i++) {
      splitGroups.add(new ArrayList<BlockMetaData>());
    }
    for (BlockMetaData block : blocks) {
      final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
      int index = Arrays.binarySearch(hdfsBlocks, new BlockLocation() {@Override
        public long getOffset() {
        return firstDataPage;
      }}, comparator);
      if (index >= 0) {
        splitGroups.get(index).add(block);
      } else {
        int insertionPoint = - index - 1;
        if (insertionPoint == 0) {
          // really, there should always be a block in 0
          LOG.warn("row group before the first HDFS block:  " + block);
          splitGroups.get(0).add(block);
        } else {
          splitGroups.get(insertionPoint - 1).add(block);
        }
      }
    }
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < hdfsBlocks.length; i++) {
      BlockLocation hdfsBlock = hdfsBlocks[i];
      List<BlockMetaData> blocksForCurrentSplit = splitGroups.get(i);
      if (blocksForCurrentSplit.size() == 0) {
        LOG.warn("HDFS block without row group: " + hdfsBlocks[i]);
      } else {
        splits.add(new ParquetInputSplit(
          fileStatus.getPath(),
          hdfsBlock.getOffset(),
          hdfsBlock.getLength(),
          hdfsBlock.getHosts(),
          blocksForCurrentSplit,
          fileMetaData.getSchema().toString(),
          requestedSchema,
          fileMetaData.getKeyValueMetaData()
          ));
      }
    }
    return splits;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    return getSplits(jobContext.getConfiguration());
  }

  public List<InputSplit> getSplits(Configuration configuration) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<Footer> footers = getFooters(configuration);
    FileMetaData globalMetaData = getGlobalMetaData(configuration);
    ReadContext readContext = getReadSupport(configuration).init(
        configuration,
        globalMetaData.getKeyValueMetaData(),
        globalMetaData.getSchema());
    for (Footer footer : footers) {
      final Path file = footer.getFile();
      LOG.debug(file);
      FileSystem fs = file.getFileSystem(configuration);
      FileStatus fileStatus = fs.getFileStatus(file);
      ParquetMetadata parquetMetaData = footer.getParquetMetadata();
      List<BlockMetaData> blocks = parquetMetaData.getBlocks();
      BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      splits.addAll(
          generateSplits(
              blocks,
              fileBlockLocations,
              fileStatus,
              parquetMetaData.getFileMetaData(),
              readSupportClass,
              readContext.getRequestedSchema().toString())
          );
    }
    return splits;
  }

  /**
   * @param jobContext the current job context
   * @return the footers for the files
   * @throws IOException
   */
  public List<Footer> getFooters(JobContext jobContext) throws IOException {
    return getFooters(jobContext.getConfiguration());
  }

  public List<Footer> getFooters(Configuration configuration) throws IOException {
    if (footers == null) {
      List<FileStatus> statuses = listStatus(configuration);
      LOG.debug("reading " + statuses.size() + " files");
      footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, statuses);
    }
    return footers;
  }

  /**
   * @param jobContext the current job context
   * @return the merged metadata from the footers
   * @throws IOException
   */
  public FileMetaData getGlobalMetaData(JobContext jobContext) throws IOException {
    return getGlobalMetaData(jobContext.getConfiguration());
  }

  public FileMetaData getGlobalMetaData(Configuration configuration) throws IOException {
    FileMetaData fileMetaData = null;
    for (Footer footer : getFooters(configuration)) {
      ParquetMetadata currentMetadata = footer.getParquetMetadata();
      fileMetaData = mergeInto(currentMetadata.getFileMetaData(), fileMetaData);
    }
    return fileMetaData;
  }

  private static final PathFilter myHiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    };

  /**
   * Proxy PathFilter that accepts a path only if all filters given in the
   * constructor do. Used by the listPaths() to apply the built-in
   * hiddenFileFilter together with a user provided one (if any).
   */
  private static class MyMultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MyMultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  protected List<FileStatus> listStatus(Configuration conf
                                          ) throws IOException {
      List<FileStatus> result = new ArrayList<FileStatus>();

      String [] list = StringUtils.split(conf.get("mapred.input.dir", ""));
      Path[] dirs = new Path[list.length];
      for (int i = 0; i < list.length; i++) {
         dirs[i] = new Path(StringUtils.unEscapeString(list[i]));
      }

      if (dirs.length == 0) {
        throw new IOException("No input paths specified in job");
      }

      List<IOException> errors = new ArrayList<IOException>();

      // creates a MultiPathFilter with the hiddenFileFilter and the
      // user provided one (if any).
      List<PathFilter> filters = new ArrayList<PathFilter>();
      filters.add(myHiddenFileFilter);

      Class<?> filterClass = conf.getClass("mapred.input.pathFilter.class", null,
           PathFilter.class);
      PathFilter jobFilter = (filterClass != null) ?
           (PathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;

      if (jobFilter != null) {
        filters.add(jobFilter);
      }
      PathFilter inputFilter = new MyMultiPathFilter(filters);

      for (int i=0; i < dirs.length; ++i) {
        Path p = dirs[i];
        FileSystem fs = p.getFileSystem(conf);
        FileStatus[] matches = fs.globStatus(p, inputFilter);
        if (matches == null) {
          errors.add(new IOException("Input path does not exist: " + p));
        } else if (matches.length == 0) {
          errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
        } else {
          for (FileStatus globStat: matches) {
            if (globStat.isDir()) {
              for(FileStatus stat: fs.listStatus(globStat.getPath(),
                  inputFilter)) {
                result.add(stat);
              }
            } else {
              result.add(globStat);
            }
          }
        }
      }

      if (!errors.isEmpty()) {
        throw new InvalidInputException(errors);
      }
      LOG.info("Total input paths to process : " + result.size());
      return result;
    }
}
