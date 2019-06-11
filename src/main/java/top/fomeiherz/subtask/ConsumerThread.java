package top.fomeiherz.subtask;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.fomeiherz.util.CanalParser;
import top.fomeiherz.vo.DataChange;

import java.util.ArrayList;
import java.util.List;

public class ConsumerThread implements Runnable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private String destination;
    private CanalConnectorFactory canalConnectorFactory;

    private Integer size = 1000;

    public ConsumerThread(String destination, CanalConnectorFactory canalConnectorFactory) {
        this.destination = destination;
        this.canalConnectorFactory = canalConnectorFactory;
    }

    @Override
    public void run() {
        CanalConnector canalConnector = canalConnectorFactory.newCanalConnector(destination);
        for (; ; ) {
            try {
                // Get message
                Message message = canalConnector.get(size);
                List<DataChange> changes = getDataChanges(message);
                // TODO Do something...
                logger.debug(JSONObject.toJSONString(changes));
                // Sleep 10s
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
                if (canalConnector != null) {
                    canalConnector.disconnect();
                }
                canalConnector = canalConnectorFactory.newCanalConnector(destination);
            }
        }
    }

    private List<DataChange> getDataChanges(Message message) {
        long id = message.getId();
        List<DataChange> dataChanges = new ArrayList<>();
        // Has data?
        if (id == -1 || id == 0) {
            return dataChanges;
        }
        for (CanalEntry.Entry entry : message.getEntries()) {
            // Only deal rowdata type
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            CanalEntry.EventType type = entry.getHeader().getEventType();
            // Only deal insert/update/delete type
            if (type == null || (type != CanalEntry.EventType.INSERT && type != CanalEntry.EventType.DELETE && type != CanalEntry.EventType.UPDATE)) {
                continue;
            }
            dataChanges.addAll(CanalParser.parse(entry));
        }
        return dataChanges;
    }

}
