package dk.dma.aisbus;

import dk.frv.ais.filter.MessageDoubletFilter;
import dk.frv.ais.filter.MessageDownSample;
import dk.frv.ais.handler.IAisHandler;
import dk.frv.ais.proprietary.DmaFactory;
import dk.frv.ais.proprietary.GatehouseFactory;
import dk.frv.ais.reader.AisReader;

public class TcpReader extends BusProviderComponent {

	private AisReader aisReader;	

	public TcpReader(AisReader aisReader, MessageBus messageBus) {
		super(messageBus);
		this.aisReader = aisReader;
	}

	public void start() {
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

		// Start reader
		aisReader.addProprietaryFactory(new DmaFactory());
		aisReader.addProprietaryFactory(new GatehouseFactory());
		aisReader.registerHandler(handler);
		aisReader.start();
	}

	public void setAisReader(AisReader aisReader) {
		this.aisReader = aisReader;
	}
	
}
