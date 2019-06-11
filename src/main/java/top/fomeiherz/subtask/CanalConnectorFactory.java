package top.fomeiherz.subtask;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;

/**
 * Destination: example
 */
@Component
public class CanalConnectorFactory {

    @Value("${canal.host}")
    private String canalHost;
    @Value("${canal.port}")
    private String canalPort;
    @Value("${canal.username}")
    private String canalUsername;
    @Value("${canal.password}")
    private String canalPassword;
    @Value("${canal.zk.address}")
    private String zkAddress;

    /**
     * Get canal connector by destination.
     *
     * @param destination
     * @return
     */
    public CanalConnector newCanalConnector(String destination) {
        CanalConnector connector;
        if (StringUtils.isNotBlank(zkAddress)) {
            // Cluster connector
            connector = CanalConnectors.newClusterConnector(zkAddress, destination, canalUsername, canalPassword);
        } else {
            // Single connector
            connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalHost, Integer.valueOf(canalPort)), destination, canalUsername, canalPassword);
        }

        connector.connect();
        connector.subscribe();

        return connector;
    }

}
