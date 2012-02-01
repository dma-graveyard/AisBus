package dk.dma.aisbus;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;

import dk.frv.ais.message.AisMessage;

public class BusComponent {
	
	protected MessageBus messageBus;
	protected int downsamplingRate;
	protected int doubleFilterWindow;
	protected Set<Integer> messageFilter = null;
	protected boolean gzipCompress = false;
	
	public BusComponent(MessageBus messageBus) {
		this.messageBus = messageBus;
	}
	
	public void setMessageFilter(String idsStr) {
		String[] ids = StringUtils.split(idsStr, ",");
		if (ids.length > 0) {
			messageFilter = new TreeSet<Integer>();
			for (String id : ids) {
				messageFilter.add(Integer.parseInt(id));
			}
		}
	}
	
	public boolean isFilterAllowed(AisMessage aisMessage) {
		if (messageFilter == null) return true;
		return messageFilter.contains(aisMessage.getMsgId());
	}
	
	public void setDoubleFilterWindow(int doubleFilterWindow) {
		this.doubleFilterWindow = doubleFilterWindow;
	}

	public void setDownsamplingRate(int downsamplingRate) {
		this.downsamplingRate = downsamplingRate;
	}

	public int getDoubleFilterWindow() {
		return doubleFilterWindow;
	}
	
	public int getDownsamplingRate() {
		return downsamplingRate;
	}
	
	public void setMessageBus(MessageBus messageBus) {
		this.messageBus = messageBus;
	}
	
	public MessageBus getMessageBus() {
		return messageBus;
	}
	
	public Set<Integer> getMessageFilter() {
		return messageFilter;
	}
	
	public void setMessageFilter(Set<Integer> messageFilter) {
		this.messageFilter = messageFilter;
	}
	
	public boolean isGzipCompress() {
		return gzipCompress;
	}
	
	public void setGzipCompress(boolean gzipCompress) {
		this.gzipCompress = gzipCompress;
	}

}
