package com.ociweb.canter;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BounceStage extends PronghornStage {

	private final Pipe<ReleaseSchema>[] releaseAfterParse;
	private final Pipe<NetPayloadSchema>[] receivedFromNet;
	private final Pipe<NetPayloadSchema>[] sendingToNet;

	private int sequenceNo = -1;
	private long fieldConnectionID = -1;
	
	public BounceStage(GraphManager graphManager, 
			Pipe<NetPayloadSchema>[] receivedFromNet,
			Pipe<ReleaseSchema>[] releaseAfterParse,
			Pipe<NetPayloadSchema>[] sendingToNet) {
		
		super(graphManager, receivedFromNet, join(releaseAfterParse, sendingToNet));
		
		this.receivedFromNet = receivedFromNet;
		this.releaseAfterParse = releaseAfterParse;
		this.sendingToNet = sendingToNet;
		
		assert(receivedFromNet.length == releaseAfterParse.length);
		assert(releaseAfterParse.length == sendingToNet.length);
		
	}

	@Override
	public void run() {
		
		int i = receivedFromNet.length;
		while (--i>=0) {
			process(receivedFromNet[i], releaseAfterParse[i], sendingToNet[i]);
		}
				
	}

	
	private void process(Pipe<NetPayloadSchema> fromNet, 
			             Pipe<ReleaseSchema> release, 
			             Pipe<NetPayloadSchema> toNet) {
		
		while (Pipe.hasContentToRead(fromNet) &&
			   Pipe.hasRoomForWrite(toNet)	) {
			
			if (Pipe.peekMsg(fromNet, NetPayloadSchema.MSG_BEGIN_208)) {
				sequenceNo = Pipe.peekInt(fromNet, 1);
			}
			
			if (Pipe.peekMsg(fromNet, NetPayloadSchema.MSG_ENCRYPTED_200)) {
				throw new UnsupportedOperationException("Can no bounce encrypted data, must be decrypted first to be re-encrypted later.");
			}
			
			if (Pipe.peekMsg(fromNet,
					         NetPayloadSchema.MSG_PLAIN_210,
					         NetPayloadSchema.MSG_DISCONNECT_203,
					         NetPayloadSchema.MSG_UPGRADE_307)) {
				fieldConnectionID = Pipe.peekLong(fromNet, 1);
			}
			
			Pipe.copyFragment(fromNet, toNet);	
						
			ReleaseSchema.publishReleaseWithSeq(release, 
											 fieldConnectionID, 
											 Pipe.getWorkingTailPosition(fromNet), 
											 sequenceNo);

		}		
	}

}
