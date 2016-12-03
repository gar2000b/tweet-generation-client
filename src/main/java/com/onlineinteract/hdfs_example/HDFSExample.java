package com.onlineinteract.hdfs_example;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class HDFSExample {

	public static void main(String[] args) throws IOException,
			URISyntaxException {
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "hdfs://localhost:9000");
		FileSystem hdfs = FileSystem.get(configuration);
		
		Path file = new Path("hdfs://localhost:9000/s2013/batch/table.html");
		if (hdfs.exists(file)) {
			hdfs.delete(file, true);
		}
		
		DataOutputStream os = hdfs.create(file, new Progressable() {
			public void progress() {
				System.out.println("Some bytes were written");
			}
		});
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		br.write("Some data 1\n");
		br.write("Some data 2\n");
		br.write("Some data 3\n");
		br.close();
		hdfs.close();
		
		System.out.println("Application complete");
	}
}
