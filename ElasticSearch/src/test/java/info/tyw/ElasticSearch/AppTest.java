package info.tyw.ElasticSearch;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
	
	/**
	 * 测试创建一个Node节点，加入集群中,通过这个node获取client(不建议使用),这种方式相当于创建一个节点，不存储数据集
	 * 可以通过classpath下的elasticseach.yml设置，可以通关编程方式配置
	 * 
	 */
	@Test
	public void testCreateNode() {
//		Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ping_timeout", 1000)
//                .put("discovery.zen.ping.multicast.enabled", "false").put("timeout", 1)
//                .putArray("discovery.zen.ping.unicast.hosts", "l-flightdev18.f.dev.cn0.qunar.com:9300", "l-flightdev17.f.dev.cn0.qunar.com:9300")
//                .build();
//        Node node = NodeBuilder.nodeBuilder().clusterName("flight_fuwu_order_index").client(true).settings(settings).node();
//        Client client = node.client();
	}
    
}
