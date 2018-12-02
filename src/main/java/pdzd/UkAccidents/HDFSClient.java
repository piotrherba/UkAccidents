package pdzd.UkAccidents;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSClient {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
//		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
//		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		
		HDFSClient object = new HDFSClient();
//		FileSystem fs = FileSystem.get(conf);
		
		object.makeDirectory("user/cloudera/test1",conf);

	}

	public void makeDirectory(String dirPath, Configuration conf) throws IOException{
		
		FileSystem fs = FileSystem.get(conf);
		
		Path path = new Path(dirPath);
		
		if(fs.exists(path)){
			System.out.println("Directory: " + dirPath + " already exists!");
			return;
		}
		
		fs.mkdirs(path);
		
	}
}
