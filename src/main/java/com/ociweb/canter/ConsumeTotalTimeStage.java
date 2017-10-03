package com.ociweb.canter;

import org.HdrHistogram.Histogram;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.SSLConnection;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FragmentWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConsumeTotalTimeStage extends PronghornStage {

	private final ClientCoordinator ccm; 
	private final Pipe<NetPayloadSchema>[] response;
	private final Pipe<ReleaseSchema> ackRelease;
	
	private long connectionId;
	
	public ConsumeTotalTimeStage(GraphManager gm, 
			ClientCoordinator ccm, 
			Pipe<NetPayloadSchema>[] response,
			Pipe<ReleaseSchema> ackRelease) {
		
		super(gm, response, ackRelease);
		
		this.ccm = ccm;
		this.response = response;
		this.ackRelease = ackRelease;
		this.connectionId = -1;
		
	}

	@Override
	public void run() {
		
		Pipe<NetPayloadSchema> pipe = response[0];
		
		if (Pipe.hasContentToRead(pipe)) {
			int msgIdx = Pipe.takeMsgIdx(pipe);
		    
			if (NetPayloadSchema.MSG_PLAIN_210 == msgIdx) {
				
				long connectionId = Pipe.takeLong(pipe);
				if (this.connectionId<0) {
					this.connectionId = connectionId;
				} else {
					if (this.connectionId != connectionId) {
						
						recordResults(this.connectionId);
						
						this.connectionId = connectionId;
					}
				}
				
				long arrivalTime = Pipe.takeLong(pipe);
				long position = Pipe.takeLong(pipe);
				

				
				
				DataInputBlobReader<NetPayloadSchema> payload = Pipe.openInputStream(pipe);
				
				
				long clockInMs = DataInputBlobReader.readPackedLong(payload);
				long timeSent  = DataInputBlobReader.readPackedLong(payload);
				long instance  = DataInputBlobReader.readPackedLong(payload);
				
				//TODO: read the payload...
				
				
				
				
			    Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance,NetPayloadSchema.MSG_PLAIN_210));
			    Pipe.releaseReadLock(pipe);
			    
			    
			    Pipe.presumeRoomForWrite(ackRelease);
			    FragmentWriter.writeLL(ackRelease,
			    		ReleaseSchema.MSG_RELEASE_100,
			    		connectionId,
			    		position
			    		);
			    				
			} else {
				
				throw new UnsupportedOperationException("unknown id "+msgIdx);
				
			}
			
			
		
		}
		
	}
	
	@Override
	public void shutdown() {
		
		recordResults(connectionId);
		
	}

	private void recordResults(long c) {
		ClientConnection connection = (ClientConnection)ccm.connectionForSessionId(c);
	    //TODO: the above has built in timing??
		Histogram histogram = connection.histogram();
	}

}
