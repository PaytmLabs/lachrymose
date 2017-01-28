package com.paytm.hero.marketing

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by adammuise on 2017-01-27.
  */
object HDFSHelper {

  def write(uri: String, filePath: String, data: Array[Byte], user: String) = {
    System.setProperty("HADOOP_USER_NAME", user)
    val path = new Path(filePath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    fs.close()
  }

}
