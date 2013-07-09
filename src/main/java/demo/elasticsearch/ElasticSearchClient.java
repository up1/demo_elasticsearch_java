package demo.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Date;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

public class ElasticSearchClient {
	
	private static final String INDEX = "test_search";

	public static void main(String[] args) throws Exception {
		addDocument();
		getProductById();
		searchProduct();
		deleteProductById();
	}
	
	private static Client getClusterClient() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elasticsearch")
				.put("client.transport.sniff", true)
				.build();
		TransportClient transportClient = new TransportClient(settings);
		transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		return (Client)transportClient;
	}
	
	private static Client getLocalClient() {
		Node node = nodeBuilder().local(true).node();
		return node.client();
	}
	
	public static void addDocument() throws Exception {
		Client client = getClusterClient();
		
		IndexResponse response = client.prepareIndex(INDEX, "products", "1")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("product_name", "kimchy")
		                        .field("product_description", "trying out Elastic Search")
		                        .field("product_price", "500.00")
		                        .field("create_date", new Date())		                        
		                    .endObject()
		                  )
		        .execute()
		        .actionGet();
		
		System.out.println( "Document ID = " + response.getId() );
	}
	
	public static void getProductById() throws Exception {
		Client client = getClusterClient();
		GetResponse response = client.prepareGet(INDEX, "products", "1")
		        .execute()
		        .actionGet();
		System.out.println( "Document Id = " + response.getId() );
		System.out.println( "Product Name = " + response.getSource().get("product_name") );
		System.out.println( "Product Description = " + response.getSource().get("product_description") );
		System.out.println( "Product Price = " + response.getSource().get("product_price") );
		
	}
	public static void searchProduct() {
		try {
		Client client = getClusterClient();
		SearchResponse response = client.prepareSearch(INDEX)
		        .setTypes("products")
		        .setQuery(QueryBuilders.termQuery("multi", "kimchy"))             // Query
		        .setFrom(0).setSize(60).setExplain(true)
		        .execute()
		        .actionGet();
		
//		SearchHits searchHits = response.getHits();
//		System.out.println(searchHits.getTotalHits());
//		for (SearchHit searchHit : searchHits) {
//			System.out.println("Document ID = " +searchHit.getId());
//			System.out.println( "Product Name = " + searchHit.getSource().get("product_name") );
//			System.out.println( "Product Description = " + searchHit.getSource().get("product_description") );
//			System.out.println( "Product Price = " + searchHit.getSource().get("product_price") );
//		}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void deleteProductById() throws Exception {
		Client client = getClusterClient();
		DeleteResponse response = client.prepareDelete(INDEX, "products", "1")
		        .execute()
		        .actionGet();
		System.out.println(response.getId());
	}

}
