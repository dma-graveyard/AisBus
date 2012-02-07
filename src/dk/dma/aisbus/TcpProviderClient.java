package dk.dma.aisbus;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import dk.frv.ais.message.AisMessage;

public class TcpProviderClient extends BusConsumer {

	private static final Logger LOG = Logger.getLogger(TcpProviderClient.class);

	private Socket socket;

	public TcpProviderClient(MessageBus messageBus, Socket socket) {
		super(messageBus);
		this.socket = socket;
	}

	@Override
	public void run() {
		handlerInit();
		
		addToBus();

		try {
			OutputStream outputStream;
			if (isGzipCompress()) {
				outputStream = new GZIPOutputStream(socket.getOutputStream(), gzipBufferSize);
			} else {
				outputStream = socket.getOutputStream();
			}			
			
			PrintWriter out = new PrintWriter(outputStream);
			while (!out.checkError()) {
				AisMessage aisMessage = queue.take();				
				if (isFilterAllowed(aisMessage)) {
					out.print(aisMessage.reassemble());				
				}
			}
		} catch (IOException e) {
			LOG.info("Connection closed from provider client");
		} catch (InterruptedException e) {
			LOG.error("Error when sending to provider client " + e.getMessage());
		} finally {
			try {
				socket.close();
			} catch (IOException e) { }
		}

		removeFromBus();
	}

}
