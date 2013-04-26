package parquet.cascading;

import parquet.example.data.Group;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.ParquetInputFormat;

public class DeprecatedExampleInputFormat extends DeprecatedParquetInputFormat<Group> {

  public DeprecatedExampleInputFormat() {
    realInputFormat = new ParquetInputFormat<Group>(GroupReadSupport.class);
  }

}
