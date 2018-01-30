package info.tyw.ElasticSearch;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author hephe
 *
 */
public class ElasticSearchTest {

	private TransportClient client;// TransportClient类
	private IndexRequest source;// IndexRequset类
	private static final String host = "";
	private static final Integer port = 9300;

	/**
	 * @throws UnknownHostException
	 * 
	 */
	@Test
	public void testCreateNode() throws UnknownHostException {
		// 创建客户端,使用默认的集群名,elasticSearch
		client = TransportClient.builder().build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));

	}

	/**
	 * 通过setting对象指定集群配置信息, 配置的集群名
	 */
	@Before
	public void testCreateByConfigClusterName() {
		Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch_wenbronk") // 设置集群名
				// .put("client.transport.sniff", true) // 开启嗅探 , 开启后会一直连接不上, 原因未知
				// .put("network.host", "192.168.50.37")
				.put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
				// .put("client.transport.nodes_sampler_interval", 5) //报错,
				// .put("client.transport.ping_timeout", 5) // 报错, ping等待时间,
				.build();
		client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("192.168.50.37", 9300)));
		// 默认5s
		// 多久打开连接, 默认5s
		System.out.println("success connect");
	}

	/**
	 * 测试获取集群信息
	 */
	@Test
	public void testShowClusterInfo() {
		List<DiscoveryNode> discoveryNodes = client.connectedNodes();
		for (DiscoveryNode discoveryNode : discoveryNodes) {
			System.out.println(discoveryNode.getHostAddress());
		}
	}

	/**
	 * 测试通过ES Helper创建json方式
	 * 
	 * @throws IOException
	 */
	@Test
	public void testCreateJsonByESHelperClass() throws IOException {
		// 创建json对象, 其中一个创建json的方式
		XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("user", "kimchy")
				.field("postDate", new Date()).field("message", "trying to out ElasticSearch").endObject();
		System.out.println(source);
	}

	/**
	 * 测试连接ElasticSearch服务器
	 * 
	 * @throws IOException
	 */
	@Test
	public void testConnectToElasticSearch() throws IOException {
		XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("user", "kimchy")
				.field("postDate", new Date()).field("message", "trying to out ElasticSearch").endObject();
		// 存json入索引
		IndexResponse indexResponse = client.prepareIndex("index1", "type1", "1").setSource(source).get();
		// 获取结果
		String index = indexResponse.getIndex();
		String type = indexResponse.getType();
		String id = indexResponse.getId();
		long version = indexResponse.getVersion();
		boolean isCreated = indexResponse.isCreated();
		System.out.println(index + "->" + type + "->" + id + "->" + version + "->" + isCreated);

	}

	/**
	 * GET API 获取指定文档信息
	 * 
	 */
	@Test
	public void getApiDocInfo() {
		GetResponse response = client.prepareGet("index1", "type1", "1").setOperationThreaded(false).get();
		System.out.println(response.getSourceAsString());
	}

	/**
	 * 测试 delete api
	 */
	@Test
	public void deleteApiDocInfo() {
		DeleteResponse response = client.prepareDelete("index1", "type1", "1").get();
	}

	/**
	 * 测试更新 update api 使用updateRequest对象
	 * 
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * 
	 */
	@Test
	public void testUpdateApi() throws IOException, InterruptedException, ExecutionException {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("twitter");
		updateRequest.type("tweet");
		updateRequest.id("1");
		updateRequest.doc(XContentFactory.jsonBuilder().startObject()
				// 对没有的字段添加, 对已有的字段替换
				.field("gender", "male").field("message", "hello").endObject());
		UpdateResponse response = client.update(updateRequest).get();

		// 打印
		String index = response.getIndex();
		String type = response.getType();
		String id = response.getId();
		long version = response.getVersion();
		System.out.println(index + " : " + type + ": " + id + ": " + version);
	}

	/**
	 * 测试update api 使用client
	 * 
	 */
	@Test
	public void testUpdate() throws Exception {
		// 使用updateRequest对象及document进行更新
		UpdateResponse response = client.update(new UpdateRequest("twitter", "tweet", "1")
				.doc(XContentFactory.jsonBuilder().startObject().field("gender", "male").endObject())).get();
		System.out.println(response.getIndex());
	}
}
