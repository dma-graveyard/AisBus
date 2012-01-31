package dk.dma.aisbus;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

public class AisBus {
	
	private static Logger LOG;
	
	private static Settings settings = new Settings();

	public static void main(String[] args) {
		DOMConfigurator.configure("log4j.xml");
		LOG = Logger.getLogger(AisBus.class);
		LOG.info("Starting AisBus");
		
		// Load configuration
		String propsFile = "aisbus.properties";
		if (args.length > 0) {
			propsFile = args[0];
		}
		try {
			settings.load(propsFile);
		} catch (IOException e) {
			LOG.error("Failed to load settings: " + e.getMessage());
			System.exit(-1);
		}
		
		// Start all TCP readers
		LOG.info("Starting TCP readers");
		for (TcpReader tcpReader : settings.getTcpReaders().values()) {
			tcpReader.start();
		}
		
		while(true) {
			sleep(10000);
			
			// Maintanaince
		}


	}
	
	public static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
