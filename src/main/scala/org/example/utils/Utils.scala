package org.example.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.io.File

class Utils {

  private val DIRECTORY = "data/"

  def clearFilesAndDirectories ( source: String, extension: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    val srcPath = new Path(s"$DIRECTORY/$source")
    val destPath = new Path(s"$DIRECTORY/$source$extension")
    val srcFile = FileUtil.listFiles(new File(s"$DIRECTORY/$source"))
      .filter(f => f.getPath.endsWith(extension))(0)

    //Copy the CSV file outside of Directory and rename
    FileUtil.copy(srcFile, hdfs, destPath, true, hadoopConfig)

    //Gets unnecessary file to be deleted
    val fileToDelete = FileUtil.listFiles(new File(DIRECTORY))
      .filter(f => f.getPath.endsWith(".crc"))(0)

    //Removes unnecessary file
    hdfs.delete(new Path(fileToDelete.getPath),true)

    //Remove Directory created by data.write()
    hdfs.delete(srcPath, true)
  }
}
