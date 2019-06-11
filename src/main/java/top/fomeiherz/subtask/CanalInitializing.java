package top.fomeiherz.subtask;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CanalInitializing implements InitializingBean {

    private final CanalConnectorFactory canalConnectorFactory;

    @Value("${canal.destinations}")
    private String destinations;

    @Autowired
    public CanalInitializing(CanalConnectorFactory canalConnectorFactory) {
        this.canalConnectorFactory = canalConnectorFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        for (String destination: destinations.split(",")) {
            new Thread(new ConsumerThread(destination, canalConnectorFactory)).start();
        }
    }

}
