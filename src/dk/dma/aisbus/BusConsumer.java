package dk.dma.aisbus;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import dk.frv.ais.filter.MessageDoubletFilter;
import dk.frv.ais.filter.MessageDownSample;
import dk.frv.ais.handler.IAisHandler;
import dk.frv.ais.message.AisMessage;

public abstract class BusConsumer extends Thread implements IAisHandler {
	
	private static final Logger LOG = Logger.getLogger(BusConsumer.class);
	
	public static final int QUEUE_SIZE = 1000;
	
	protected BlockingQueue<AisMessage> queue = new ArrayBlockingQueue<AisMessage>(QUEUE_SIZE);
	private int downsamplingRate;
	private int doubleFilterWindow;
	private MessageBus messageBus;
	private Date lastInsertErrorReported = new Date(0);
	
	protected IAisHandler handler;
	
	public BusConsumer(MessageBus messageBus) {
		this.messageBus = messageBus;
	}
	
	@Override
	public void receive(AisMessage aisMessage) {
		try {
			queue.add(aisMessage);
		} catch (IllegalStateException e) {
			Date now = new Date();			
			if (now.getTime() - lastInsertErrorReported.getTime() > 60000) {
				lastInsertErrorReported = now;
				LOG.error("Failed to enqueue message. Queue overflow.");
			}			
		}
	}
	
	public void writeMessage(AisMessage aisMessage) {
		handler.receive(aisMessage);
	}
	
	protected void handlerInit() {
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
	
	protected void addToBus() {
		messageBus.addConsumer(this);
	}
	
	protected void removeFromBus() {
		messageBus.removeConsumer(this);
	}
		
	public void setDoubleFilterWindow(int doubleFilterWindow) {
		this.doubleFilterWindow = doubleFilterWindow;
	}
	
	public void setDownsamplingRate(int downsamplingRate) {
		this.downsamplingRate = downsamplingRate;
	}

}
