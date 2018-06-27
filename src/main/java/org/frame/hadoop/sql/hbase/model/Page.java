package org.frame.hadoop.sql.hbase.model;

import java.util.List;
import java.util.Map;

import org.frame.common.util.Properties;
import org.frame.repository.constant.IRepositoryConstant;

public class Page {
	
	private int totalCount = 0;
	
	private int pageSize = 15;
	
	private int totalPage = 0;
	
	private int currPage = 1;
	
	private int startIndex = 0;
	
	private int endIndex = 0;
	
	private List<?> data;
	
	private String table = "";
	
	private List<Map<String, Object>> parameters;
	
	private int targetPage = 1;

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getTotalPage() {
		return totalPage;
	}

	public void setTotalPage(int totalPage) {
		this.totalPage = totalPage;
	}
	
	public void setTotalPage() {
		if (totalCount % pageSize == 0) {
			this.totalPage = totalCount / pageSize;
		} else {
			this.totalPage = (totalCount / pageSize) + 1;
		}
	}

	public int getCurrPage() {
		return currPage;
	}

	public void setCurrPage(int currPage) {
		this.currPage = currPage;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}
	
	public void setStartIndex() {
		if (this.targetPage - 1 < 0) {
			this.startIndex = 0;
		} else {
			this.startIndex = (this.targetPage - 1) * this.pageSize;
		}
	}

	public int getEndIndex() {
		return endIndex;
	}

	public void setEndIndex(int endIndex) {
		this.endIndex = this.totalCount - 1;
	}
	
	public void setEndIndex() {
		if ( totalCount < pageSize) {
			this.endIndex = totalCount;
		} else if ((totalCount % pageSize == 0) || (totalCount % pageSize != 0 && targetPage < totalPage)) {
			this.endIndex = targetPage * pageSize - 1;
		} else if (totalCount % pageSize != 0 && targetPage == totalPage) {
			this.endIndex = totalCount - 1;
		}
		
		if (endIndex < 0) {
			this.endIndex = 0;
		}
		
		if (this.endIndex > this.totalCount - 1) {
			this.endIndex = this.totalCount - 1;
		}
	}

	public List<?> getData() {
		return data;
	}

	public void setData(List<?> data) {
		this.data = data;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}
	
	public int getTargetPage() {
		return targetPage;
	}

	public void setTargetPage(int targetPage) {
		this.targetPage = targetPage;
	}
	
	public List<Map<String, Object>> getParameters() {
		return parameters;
	}

	public void setParameters(List<Map<String, Object>> parameters) {
		this.parameters = parameters;
	}

	public Page() {
		this.pageSize = this.initPageSize();
	}
	
	public Page(int pageSize) {
		this.pageSize = pageSize;
	}

	public Page(String table, List<Map<String, Object>> parameters) {
		this.pageSize = this.initPageSize();
		this.setTable(table);
		this.setParameters(parameters);
	}
	
	public Page(String table, int targetPage, List<Map<String, Object>> parameters) {
		this.pageSize = this.initPageSize();
		this.setTable(table);
		this.setCurrPage(targetPage);
		this.setTargetPage(targetPage);
		this.setParameters(parameters);
	}
	
	public Page(String table, int targetPage, int pageSize, List<Map<String, Object>> parameters) {
		this.setTable(table);
		this.setCurrPage(targetPage);
		this.setTargetPage(targetPage);
		this.setPageSize(pageSize);
		this.setParameters(parameters);
	}
	
	public void init() {
		if (this.totalCount == 0) {
			this.currPage = 0;
			this.targetPage = 0;
		}
		
		this.setTotalPage();
		this.setStartIndex();
		this.setEndIndex();
	}
	
	public void refresh(String table, List<Map<String, Object>> parameters) {
		this.table = table;
		this.parameters = parameters;
	}
	
	public void reset() {
		this.totalCount = 0;
		this.totalPage = 0;
		this.currPage = 0;
		this.startIndex = 0;
		this.endIndex = 0;
		data = null;
		this.table = "";
		this.parameters = null;
		this.targetPage = 0;
	}
	
	public boolean hasData() {
		return (this.data != null && this.data.size() > 0);
	}
	
	public boolean hasNextPage() {
		return (this.currPage < this.totalPage);
	}

	public boolean hasPreviousPage() {
		return (this.currPage > 1);
	}
	
	private int initPageSize() {
		try {
			String size = String.valueOf(new Properties(IRepositoryConstant.DEFAULT_CONFIG_PROPERTIES).read(IRepositoryConstant.PAGE_SIZE)).toLowerCase();
			if (size != null && !"".equals(size)) {
				this.pageSize = Integer.parseInt(size);
			} else {
				this.pageSize = 15;
			}
		} catch (Exception e) {
			this.pageSize = 15;
			e.printStackTrace();
		}
		
		return pageSize;
	}
	
}
