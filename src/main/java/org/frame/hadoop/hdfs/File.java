package org.frame.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class File {
	
	private String path;
	
	private Configuration conf;
	
	public File(String pathname) {
		this.path = pathname.replace("\\", "/");
		if (!this.path.endsWith("/")) this.path += "/";

		String part1 = null;
		String part2 = null;
		
		int index = this.path.lastIndexOf(":");
		if (index != -1) {
			part1 = this.path.substring(0, index);
			part2 = this.path.substring(index + 1, this.path.length());
		} else {
			throw new RuntimeException("wrong path: " + path);
		}
		
		index = part2.indexOf("/");
		if (index == -1) index = part2.length();
		
		String fsDefaultName = part1 + ":" + part2.substring(0, index);
		
		conf = new Configuration();
		conf.set("fs.default.name", fsDefaultName);
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
	}
	
	public File(String master, String pathname) {
		this.path = pathname.replace("\\", "/");
		
		conf = new Configuration();
		conf.set("fs.default.name", master);
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
	}
	
	public boolean append(String content) {
		boolean result = false;
		
		FileSystem fileSystem = null;
		FSDataOutputStream fsDataOutputStream = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			fsDataOutputStream = fileSystem.append(new Path(path));
			fsDataOutputStream.writeBytes(content);
			result = true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fsDataOutputStream != null) fsDataOutputStream.close();
			} catch (IOException e) {
				fsDataOutputStream = null;
				e.printStackTrace();
			}
			
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public boolean copyFromLocal(java.io.File local) {
		return this.copyFromLocal(local.getAbsolutePath());
	}
	
	public boolean copyFromLocal(String local) {
		return this.write(false, true, local, this.path);
	}
	
	public boolean copyToLocal(java.io.File local) {
		return this.copyToLocal(local.getAbsolutePath());
	}
	
	public boolean copyToLocal(String local) {
		local = local.replace("\\", "/");
		if (!local.endsWith("/")) local += "/";
		local += this.getName() + "/";
		
		return this.read(false, local, this.path, true);
	}
	
	public boolean createNewFile() {
		boolean result = false;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			Path path = new Path(this.path);
			result = fileSystem.createNewFile(path);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public DatanodeInfo[] datanode() {
		DatanodeInfo[] result = null;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			result = ((DistributedFileSystem) fileSystem).getDataNodeStats();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public boolean delete() {
		boolean result = false;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			result = fileSystem.delete(new Path(path), true);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public boolean exists() {
		boolean result = false;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			result = fileSystem.exists(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public String getAbsolutePath() {
		return this.path;
	}
	
	public String getName() {
		return new Path(this.path).getName();
	}
	
	public boolean isDirectory() {
		boolean result = false;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			result = fileSystem.isDirectory(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public boolean isEmpty() {
		boolean result = false;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			
			FileStatus[] files = fileSystem.listStatus(new Path(path));
			result = files.length > 0 ? false : true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public boolean isFile() {
		boolean result = false;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			result = fileSystem.isFile(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public long lastModifyTime() {
		long result = -1l;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			FileStatus file = fileSystem.getFileStatus(new Path(path));
			result = file.getModificationTime();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public FileStatus[] listFile() {
		FileStatus[] result = null;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			result = fileSystem.listStatus(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public BlockLocation[] location() {
		BlockLocation[] result = null;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			FileStatus fileStatus =  fileSystem.getFileStatus(new Path(path));  
	        result = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());  
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public boolean mkdirs() {
		boolean result = false;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
	        result = fileSystem.mkdirs(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public boolean moveFromLocal(java.io.File local) {
		return this.moveFromLocal(local.getAbsolutePath());
	}
	
	public boolean moveFromLocal(String local) {
		boolean result = this.write(true, true, local, this.path);
		result = result && new org.frame.common.io.File(local).delete();
		
		return result;
	}
	
	public boolean moveToLocal(java.io.File local) {
		return this.moveToLocal(local.getAbsolutePath());
	}
	
	public boolean moveToLocal(String local) {
		boolean result = this.copyToLocal(local);
		//result = result && this.delete();
		
		return result;
	}
	
	public InputStream open() {
		InputStream result = null;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			result = fileSystem.open(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	public boolean rename(String newName) {
		boolean result = false;
		
		FileSystem fileSystem = null;
		
		try {
			fileSystem = FileSystem.get(URI.create(path), conf);
			Path oldPath = new Path(path);
			Path newPath = new Path(oldPath.getParent().toString() + "/" + newName);
			result = fileSystem.rename(oldPath, newPath);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	private boolean read(boolean delSrc, String localPath, String remotePath, boolean useRawLocalFileSystem) {
		boolean result = false;
		
		org.frame.hadoop.hdfs.File remote = new org.frame.hadoop.hdfs.File(remotePath);
		if (remote.exists()) {
			java.io.File local = new java.io.File(localPath);
			if (local.exists()) {
				if (local.isFile()) {
					result = this.readHandler(delSrc, localPath, remotePath, useRawLocalFileSystem);
				} else if (local.isDirectory()) {
					localPath = localPath.replace("\\", "/");
					remotePath = remotePath.replace("\\", "/");
					
					if (!localPath.endsWith("/")) localPath += "/";
					if (!remotePath.endsWith("/")) remotePath += "/";
					
					FileStatus[] sub = remote.listFile();
					for (FileStatus file : sub) {
						result = this.read(delSrc, localPath + file.getPath().getName(), file.getPath().toString(), useRawLocalFileSystem);
					}
				} else {
					System.err.println("local file error: " + localPath + "neither a file nor a directory, please check on it.");
				}
			} else {
				result = this.readHandler(delSrc, localPath, remotePath, useRawLocalFileSystem);
			}
		} else {
			System.err.println("file not found: " + remotePath);
		}
		
		return result;
	}
	
	private boolean readHandler(boolean delSrc, String localPath, String remotePath, boolean useRawLocalFileSystem) {
		boolean result = false;
		
		FileSystem fileSystem = null;

		try {
			fileSystem = FileSystem.get(conf);
			fileSystem.copyToLocalFile(delSrc, new Path(remotePath), new Path(localPath), useRawLocalFileSystem);
			result = true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	private boolean write(boolean delsrc, boolean override, String localPath, String remotePath) {
		boolean result = false;
		
		java.io.File local = new java.io.File(localPath);
		if (local.exists()) {
			if (local.isFile()) {
				result = this.writeHandler(delsrc, override, localPath, remotePath);
			} else if (local.isDirectory()){
				localPath = localPath.replace("\\", "/");
				remotePath = remotePath.replace("\\", "/");
				
				if (!localPath.endsWith("/")) localPath += "/";
				if (!remotePath.endsWith("/")) remotePath += "/";
				
				org.frame.hadoop.hdfs.File remote = new org.frame.hadoop.hdfs.File(remotePath + local.getName());
				if (remote.exists()) {
					java.io.File[] sub = local.listFiles();
					for (java.io.File file : sub) {
						result = this.write(delsrc, override, file.getAbsolutePath(), remotePath + local.getName());
					}
				} else {
					result = this.writeHandler(delsrc, override, localPath, remotePath);
				}
			} else {
				System.err.println("local file error: " + localPath + "neither a file nor a directory, please check on it.");
			}
		} else {
			System.err.println("file not found: " + localPath);
		}
		
		return result;
	}
	
	private boolean writeHandler(boolean delsrc, boolean override, String localPath, String remotePath) {
		boolean result = false;
		
		FileSystem fileSystem = null;

		try {
			fileSystem = FileSystem.get(conf);
			fileSystem.copyFromLocalFile(delsrc, override, new Path(localPath), new Path(remotePath));
			result = true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileSystem != null) fileSystem.close();
			} catch (IOException e) {
				fileSystem = null;
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	
	public static void main(String[] args) {
		String path = "hdfs://192.168.15.169:9000/user/root/input";
		File file = new File(path);
		System.out.println("mkdirs: " + file.mkdirs());
		System.out.println("exists: " + file.exists());
		System.out.println("isDirectory: " + file.isDirectory());
		System.out.println("isEmpty: " + file.isEmpty());
		
		/*String path2 = "hdfs://10.10.10.230:9000/user/root/input2/aa.txt";
		File file2 = new File(path2);
		System.out.println("create: " + file2.createNewFile());
		System.out.println("file exists: " + file2.exists());
		System.out.println("file isFile: " + file2.isFile());
		System.out.println("isEmpty: " + file.isEmpty());
		System.out.println("append: " + file2.append("aaa"));
		System.out.println("rename: " + file2.rename("bb.txt"));
		
		
		System.out.println("delete: " + file.delete());
		System.out.println("exists: " + file.exists());
		
		String localPath = "d://user";
		String remotePath = "hdfs://10.10.10.230:9000/user/root/input2";
		
		File file3 = new File(remotePath);
		//System.out.println("copyFromLocal: " + file3.copyFromLocal(localPath));
		//System.out.println("moveFromLocal: " + file3.moveFromLocal(localPath));
		System.out.println("moveToLocal: " + file3.moveToLocal(localPath));
		//System.out.println("delete: " + file3.delete());*/
		
		/*String path = "/user/hive/warehouse/test1/test1.txt";
		File file = new File("10.10.10.230:9000", path);
		System.out.println(file.append("1 1 1"));*/
	}
	
}
