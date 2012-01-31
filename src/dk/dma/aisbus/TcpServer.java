package dk.dma.aisbus;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

import dk.frv.ais.filter.MessageDoubletFilter;
import dk.frv.ais.filter.MessageDownSample;
import dk.frv.ais.handler.IAisHandler;
import dk.frv.ais.message.AisMessage;
import dk.frv.ais.reader.AisStreamReader;

public class TcpServer extends Thread implements IAisHandler {
	
	private static final Logger LOG = Logger.getLogger(TcpServer.class);
	
	private MessageBus messageBus;
	private int downsamplingRate;
	private int doubleFilterWindow;
	private int port;
	private int timeout;
	
	public TcpServer(MessageBus messageBus) {
		this.messageBus = messageBus;
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
				LOG.info("TCP server received connection from " + socket.getRemoteSocketAddress());
				
				AisStreamReader streamReader = new AisStreamReader(socket.getInputStream());
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
		messageBus.push(aisMessage);
	}
	
	public void setDoubleFilterWindow(int doubleFilterWindow) {
		this.doubleFilterWindow = doubleFilterWindow;
	}

	public void setDownsamplingRate(int downsamplingRate) {
		this.downsamplingRate = downsamplingRate;
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
