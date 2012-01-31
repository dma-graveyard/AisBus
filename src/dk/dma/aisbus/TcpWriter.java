package dk.dma.aisbus;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.log4j.Logger;

import dk.frv.ais.handler.IAisHandler;
import dk.frv.ais.message.AisMessage;

public class TcpWriter extends BusConsumer implements IAisHandler {

	private static final Logger LOG = Logger.getLogger(TcpWriter.class);

	private int port;
	private String host;

	public TcpWriter(MessageBus messageBus) {
		super(messageBus);
	}

	@Override
	public void run() {
		handlerInit();

		while (true) {
			Socket socket = new Socket();
			try {
				InetSocketAddress address = new InetSocketAddress(host, port);
				LOG.info("Connecting to " + host + ":" + port + " ...");
				socket.connect(address);
				socket.setKeepAlive(true);
				PrintWriter out = new PrintWriter(socket.getOutputStream());
				LOG.info("Connected.");

				while (!out.checkError()) {
					// Wait for message and write
					AisMessage aisMessage = queue.take();
					out.print(aisMessage.getVdm().getOrgLinesJoined());
				}

			} catch (IOException e) {
				LOG.error("TCP writer connection failed: " + e.getMessage());
			} catch (InterruptedException e) {
				LOG.error("TCP writer connection failed: " + e.getMessage());
			} finally {
				try {
					socket.close();
				} catch (IOException e) { }
			}
			
			AisBus.sleep(5000);

		}
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getPort() {
		return port;
	}

	public String getHost() {
		return host;
	}

}
