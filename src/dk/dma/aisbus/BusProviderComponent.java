package dk.dma.aisbus;

import dk.frv.ais.handler.IAisHandler;
import dk.frv.ais.message.AisMessage;
import dk.frv.ais.proprietary.DmaSourceTag;

public abstract class BusProviderComponent extends BusComponent implements IAisHandler {
	
	protected String sourceName = null;
	
	public BusProviderComponent(MessageBus messageBus) {
		super(messageBus);
	}
	
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
	
	@Override
	public void receive(AisMessage aisMessage) {
		if (isFilterAllowed(aisMessage)) {
			// Maybe make DMA source tag
			if (sourceName != null) {
				DmaSourceTag dmaSourceTag = new DmaSourceTag();
				dmaSourceTag.setSourceName(sourceName);
				aisMessage.setTag(dmaSourceTag);
			}			
			messageBus.push(aisMessage);
		}
	}

}
