package com.hadoop.filesystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ScanFiles {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Configuration configuration=new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(new URI("hdfs://nnfailover"), configuration);
			FileStatus[] fileStatus = hdfs.listStatus(new Path("hdfs://nnfailover//data/prptc/bdm/hive/bos/log_date=2018-06-22"));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			System.out.println("***** Contents of the Directory *****");
			for(Path path : paths)
		    {
		      System.out.println(path);
		    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
