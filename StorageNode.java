import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
	static Logger log;

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
			System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
			System.exit(-1);
		}

		String host = args[0];
		String port = args[1];
		String zkNode = args[3];
		String serverAddress = host + ":" + port;

		CuratorFramework curClient = CuratorFrameworkFactory.builder().connectString(args[2])
				.retryPolicy(new RetryNTimes(10, 1000)).connectionTimeoutMs(1000).sessionTimeoutMs(10000).build();

		curClient.start();
		curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkNode + "/s-", serverAddress.getBytes());

		TServerSocket socket = new TServerSocket(Integer.parseInt(port));
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());

		// start curator cache to listen events
		PathChildrenCache cache = new PathChildrenCache(curClient, zkNode, true);

		cache.start();

		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(
				new KeyValueHandler(host, Integer.parseInt(port), curClient, zkNode, cache));
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new TThreadPoolServer(sargs);
		server.serve();
	}
}
