package dk.dma.aisbus;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

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
			PrintWriter out = new PrintWriter(socket.getOutputStream());
			while (!out.checkError()) {
				AisMessage aisMessage = queue.take();
				out.print(aisMessage.getVdm().getOrgLinesJoined() + "\r\n");				
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
