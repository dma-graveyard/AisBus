package dk.dma.aisbus;

import dk.frv.ais.filter.MessageDoubletFilter;
import dk.frv.ais.filter.MessageDownSample;
import dk.frv.ais.handler.IAisHandler;
import dk.frv.ais.message.AisMessage;
import dk.frv.ais.reader.AisReader;

public class TcpReader implements IAisHandler {

	private AisReader aisReader;
	private int downsamplingRate;
	private int doubleFilterWindow;

	public TcpReader(AisReader aisReader) {
		this.aisReader = aisReader;
	}

	@Override
	public void receive(AisMessage aisMessage) {
		System.out.println("received message: " + aisMessage);

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
		aisReader.registerHandler(handler);
		aisReader.start();
	}

	public void setAisReader(AisReader aisReader) {
		this.aisReader = aisReader;
	}

	public void setDoubleFilterWindow(int doubleFilterWindow) {
		this.doubleFilterWindow = doubleFilterWindow;
	}

	public void setDownsamplingRate(int downsamplingRate) {
		this.downsamplingRate = downsamplingRate;
	}

}
