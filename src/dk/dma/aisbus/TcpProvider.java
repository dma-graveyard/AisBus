package dk.dma.aisbus;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

public class TcpProvider extends BusComponent implements Runnable {

	private static final Logger LOG = Logger.getLogger(TcpProvider.class);

	private int port;

	public TcpProvider(MessageBus messageBus) {
		super(messageBus);
	}
	
	public void start() {
		(new Thread(this)).start();
	}

	@Override
	public void run() {
		// Create server socket
		ServerSocket serverSocket;
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			LOG.error("Failed to open server socket on port " + port + ": " + e.getMessage());
			return;
		}

		while (true) {
			// Listen for connections and spawn client handlers
			try {
				Socket socket = serverSocket.accept();
				socket.setKeepAlive(true);
				LOG.info("TCP provider received connection from " + socket.getRemoteSocketAddress());
				// Make consumer
				TcpProviderClient client = new TcpProviderClient(messageBus, socket);
				client.setDoubleFilterWindow(doubleFilterWindow);
				client.setDownsamplingRate(downsamplingRate);
				client.setMessageFilter(messageFilter);
				client.setGzipCompress(gzipCompress);
				client.start();				
			} catch (IOException e) {
				LOG.error("TCP provider failed: " + e.getMessage());
			}
		}

	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
}
