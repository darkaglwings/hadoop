package org.frame.hadoop.sql;

import java.util.List;
import java.util.Map;

import org.frame.hadoop.sql.hive.IHive;

public interface IHadoop extends IHive {
	
	public boolean delete(String name, String key);
    
    public boolean delete(String name, String key, String family, String qualifier);
    
    public boolean delete(String name, List<Map<String, String>> parameters);
    
    public boolean delete(Object object);
    
    public boolean delete(Object[] parameters);
    
    public boolean delete(List<Object> parameters);
    
    public boolean insert(String key, String name, String family, String qualifier, String value);
    
    public boolean insert(String name, List<Map<String, String>> parameters);
    
    public boolean insert(Object object);
    
    public boolean insert(Object[] parameters);
    
    public boolean insert(List<Object> parameters);
    
    public boolean turncate(String name, String key);
    
    public boolean update(String name, String key, String family, String qualifier, String value);
    
    public boolean update(String name, List<Map<String, String>> parameters);
    
    public boolean update(Object object);
    
    public boolean update(Object[] parameters);
    
    public boolean update(List<Object> parameters);

}
