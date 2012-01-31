package dk.dma.aisbus;

import java.util.HashSet;
import java.util.Set;

import dk.frv.ais.filter.MessageDoubletFilter;
import dk.frv.ais.filter.MessageDownSample;
import dk.frv.ais.handler.IAisHandler;
import dk.frv.ais.message.AisMessage;

public class MessageBus implements IAisHandler {

	private IAisHandler handler;
	protected Set<BusConsumer> consumers = new HashSet<BusConsumer>();
	
	public MessageBus(int doubleFilterWindow, int downsamplingRate) {
		handler = this;
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

	}

	@Override
	public synchronized void receive(AisMessage aisMessage) {
		// Output from message bus
		for (BusConsumer busConsumer : consumers) {
			busConsumer.writeMessage(aisMessage);
		}		
	}
	
	public synchronized void push(AisMessage aisMessage) {
		// Push to handler
		handler.receive(aisMessage);		
	}
	
	public synchronized void addConsumer(BusConsumer busConsumer) {
		consumers.add(busConsumer);
	}
	
	public synchronized void removeConsumer(BusConsumer busConsumer) {
		consumers.remove(busConsumer);
	}

}
