package org.frame.hadoop.sql.impl;

import java.util.List;
import java.util.Map;

import org.frame.hadoop.sql.IHadoop;
import org.frame.hadoop.sql.hbase.impl.HBase;
import org.frame.hadoop.sql.hive.impl.Hive;

public class Hadoop extends Hive implements IHadoop {
	
	public boolean delete(String name, String key) {
    	return new HBase().delete(name, key);
	}
    
    public boolean delete(String name, String key, String family, String qualifier) {
    	return new HBase().delete(name, key, family, qualifier);
    }
    
    public boolean delete(String name, List<Map<String, String>> parameters) {
    	return new HBase().delete(name, parameters);
    }
    
    public boolean delete(Object object) {
    	return new HBase().delete(object);
    }
    
    public boolean delete(Object[] parameters) {
    	return new HBase().delete(parameters);
    }
    
    public boolean delete(List<Object> parameters) {
    	return new HBase().delete(parameters);
    }
    
    public boolean insert(String key, String name, String family, String qualifier, String value) {
    	return new HBase().insert(key, name, family, qualifier, value);
    }
    
    public boolean insert(String name, List<Map<String, String>> parameters) {
    	return new HBase().insert(name, parameters);
    }
    
    public boolean insert(Object object) {
    	return new HBase().insert(object);
    }
    
    public boolean insert(Object[] parameters) {
    	return new HBase().insert(parameters);
    }
    
    public boolean insert(List<Object> parameters) {
    	return new HBase().insert(parameters);
    }
    
    public boolean turncate(String name, String key){
    	return new HBase().turncate(name, key);
    }
    
    public boolean update(String name, String key, String family, String qualifier, String value) {
    	return new HBase().update(name, key, family, qualifier, value);
    }
    
    public boolean update(String name, List<Map<String, String>> parameters) {
    	return new HBase().update(name, parameters);
    }
    
    public boolean update(Object object) {
    	return new HBase().update(object);
    }
    
    public boolean update(Object[] parameters) {
    	return new HBase().update(parameters);
    }
    
    public boolean update(List<Object> parameters) {
    	return new HBase().update(parameters);
    }

}
