package top.fomeiherz.vo;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.List;

/**
 * Canal获取的数据行变化信息
 * 
 * @author Cobe
 *
 */
public class DataChange implements Serializable {
	private static final long serialVersionUID = 6097232502679192569L;

	/**
	 * 数据变化时间戳
	 */
	private long executeTime;

	/**
	 * 变化类型(INSERT/UPDATE/DELETE)
	 */
	private ChangeType changeType;

	/**
	 * 所属数据schema
	 */
	private String schema;

	/**
	 * 所属数据表
	 */
	private String table;

	/**
	 * 变化前数据(INSERT时为null)
	 */
	private JSONObject before;

	/**
	 * 变化后数据(DELETE时为null)
	 */
	private JSONObject after;

	/**
	 * 变化字段列表
	 */
	private List<String> updates;

	public JSONObject getBefore() {
		return before;
	}

	public void setBefore(JSONObject before) {
		this.before = before;
	}

	public JSONObject getAfter() {
		return after;
	}

	public void setAfter(JSONObject after) {
		this.after = after;
	}

	public List<String> getUpdates() {
		return updates;
	}

	public void setUpdates(List<String> updates) {
		this.updates = updates;
	}

	public ChangeType getChangeType() {
		return changeType;
	}

	public void setChangeType(ChangeType changeType) {
		this.changeType = changeType;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public long getExecuteTime() {
		return executeTime;
	}

	public void setExecuteTime(long executeTime) {
		this.executeTime = executeTime;
	}

	/**
	 * 数据变化类型
	 * 
	 * @author Cobe
	 *
	 */
	public enum ChangeType {
		INSERT, UPDATE, DELETE
	}
}
