package top.fomeiherz.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.fomeiherz.vo.DataChange;

import java.util.ArrayList;
import java.util.List;

public class CanalParser {

    private static Logger logger = LoggerFactory.getLogger(CanalParser.class);

    /**
     * Parse {@link CanalEntry} to {@link DataChange}
     *
     * @param entry From canal's entries.
     * @return List<DataChange>
     */
    public static List<DataChange> parse(Entry entry) {
        List<DataChange> dataChanges = new ArrayList<>();

        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                DataChange dataChange = parseColumns(rowChange.getEventType(), rowData);
                if (dataChange != null) {
                    dataChange.setExecuteTime(entry.getHeader().getExecuteTime());
                    dataChange.setSchema(entry.getHeader().getSchemaName());
                    dataChange.setTable(entry.getHeader().getTableName());

                    dataChanges.add(dataChange);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error(e.getMessage(), e);
        }
        return dataChanges;
    }

    /**
     * Parse {@link CanalEntry.Column} to {@link DataChange}
     *
     * @param eventType  Change type
     * @param rowData Row data
     * @return {@link DataChange}
     */
    private static DataChange parseColumns(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        DataChange dataChange = new DataChange();

        if (eventType == CanalEntry.EventType.INSERT) {
            // Set change type
            dataChange.setChangeType(DataChange.ChangeType.INSERT);
            // Save after data object
            JSONObject after = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                after.put(column.getName(), column.getValue());
            }
            dataChange.setAfter(after);
        } else if (eventType == CanalEntry.EventType.UPDATE) {
            // Set change type
            dataChange.setChangeType(DataChange.ChangeType.UPDATE);

            List<String> updates = new ArrayList<>();

            // Save before data object
            JSONObject before = new JSONObject();
            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                before.put(column.getName(), column.getValue());
            }

            // Save after data object
            JSONObject after = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                after.put(column.getName(), column.getValue());
                // Is updated
                if (column.getUpdated()) {
                    updates.add(column.getName());
                }
            }
            dataChange.setBefore(before);
            dataChange.setAfter(after);
            dataChange.setUpdates(updates);
        } else if (eventType == CanalEntry.EventType.DELETE) {
            // Set change type
            dataChange.setChangeType(DataChange.ChangeType.DELETE);

            // Save before data object
            JSONObject before = new JSONObject();
            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                before.put(column.getName(), column.getValue());
            }
            dataChange.setBefore(before);
        } else {
            return null;
        }
        return dataChange;
    }

}
