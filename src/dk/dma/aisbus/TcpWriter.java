package dk.dma.aisbus;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.zip.GZIPOutputStream;

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
				
				OutputStream outputStream;
				if (isGzipCompress()) {
					outputStream = new GZIPOutputStream(socket.getOutputStream(), gzipBufferSize);
				} else {
					outputStream = socket.getOutputStream();
				}
				
				PrintWriter out = new PrintWriter(outputStream);
				LOG.info("Connected.");
				
				addToBus();

				while (!out.checkError()) {
					// Wait for message and write
					AisMessage aisMessage = queue.take();
					if (isFilterAllowed(aisMessage)) {
						String sendingMessage = aisMessage.reassemble() + "\r\n";
						System.out.print(sendingMessage);
						out.print(sendingMessage);
					}
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
			
			removeFromBus();
			
			AisBus.sleep(20000);

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
