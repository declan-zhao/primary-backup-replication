import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientManager {
    private String host;
    private int port;
    private ConcurrentLinkedQueue<KeyValueService.Client> clients;
    private final int POOL_SIZE = 10;

    public ClientManager() {
    }

    public void setServerAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void initialize(KeyValueService.Client client) {
        this.clearClients();
        this.clients.offer(client);
        this.refreshThriftClients();
    }

    private void clearClients() {
        this.clients = new ConcurrentLinkedQueue<KeyValueService.Client>();
    }

    private void refreshThriftClients() {
        for (int i = 0; i < POOL_SIZE - 1; i++) {
            KeyValueService.Client client = null;

            while (client == null) {
                client = this.createThriftClient();
            }

            this.clients.offer(client);
        }
    }

    public KeyValueService.Client createThriftClient() {
        try {
            TSocket socket = new TSocket(this.host, this.port);
            TTransport transport = new TFramedTransport(socket);

            if (!transport.isOpen()) {
                transport.open();
            }

            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client client = new KeyValueService.Client(protocol);

            return client;
        } catch (Exception e) {
            return null;
        }
    }

    public KeyValueService.Client poll() {
        while (this.clients.isEmpty()) {
        }

        return this.clients.poll();
    }

    public void offer(KeyValueService.Client client) {
        this.clients.offer(client);
    }
}
