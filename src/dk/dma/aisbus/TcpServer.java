package dk.dma.aisbus;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

import dk.frv.ais.filter.MessageDoubletFilter;
import dk.frv.ais.filter.MessageDownSample;
import dk.frv.ais.handler.IAisHandler;
import dk.frv.ais.message.AisMessage;
import dk.frv.ais.reader.AisStreamReader;

public class TcpServer extends BusComponent implements IAisHandler, Runnable {
	
	private static final Logger LOG = Logger.getLogger(TcpServer.class);
	
	private int port;
	private int timeout;
	
	public TcpServer(MessageBus messageBus) {
		super(messageBus);
	}
	
	public void start() {
		(new Thread(this)).start();
	}
	
	@Override
	public void run() {
		IAisHandler handler = this;
		// Maybe insert down sampling filter
		if (downsamplingRate > 0) {
			MessageDownSample downsample = new MessageDownSample(downsamplingRate);
			downsample.registerReceiver(handler);
			handler = downsample;
		}
		// Maybe insert doublet filtering
		if (doubleFilterWindow > 0) {
			MessageDoubletFilter doubletFilter = new MessageDoubletFilter();
			doubletFilter.setWindowSize(doubleFilterWindow);
			doubletFilter.registerReceiver(handler);
			handler = doubletFilter;
		}

		ServerSocket serverSocket;
		try {
			serverSocket = new ServerSocket(port);						
		} catch (IOException e) {
			LOG.error("Failed to open server socket on port " + port + ": " + e.getMessage());
			return;
		}
		
		while (true) {
			Socket socket = null;
			try {
				socket = serverSocket.accept();
				socket.setSoTimeout(timeout * 1000);
				socket.setKeepAlive(true);
				LOG.info("TCP server received connection from " + socket.getRemoteSocketAddress());
				
				InputStream inputStream;
				if (isGzipCompress()) {
					inputStream = new GZIPInputStream(socket.getInputStream());
				} else {
					inputStream = socket.getInputStream();
				}
				
				AisStreamReader streamReader = new AisStreamReader(inputStream);
				streamReader.registerHandler(handler);
				streamReader.start();
				streamReader.join();
				
			} catch (IOException e) {
				LOG.error("TCP server failed: " + e.getMessage());			
			} catch (InterruptedException e) {
				LOG.error("TCP server failed: " + e.getMessage());
			} finally {
				try {
					socket.close();
				} catch (IOException e) { }
			}
		}
		
	}

	@Override
	public void receive(AisMessage aisMessage) {
		if (isFilterAllowed(aisMessage)) {
			messageBus.push(aisMessage);
		}
	}
	
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public int getPort() {
		return port;
	}

}
