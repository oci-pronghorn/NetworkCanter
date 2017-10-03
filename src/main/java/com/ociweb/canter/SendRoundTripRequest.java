package com.ociweb.canter;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SendRoundTripRequest extends PronghornStage {

	private static final int sessionId = 0;
	private final Pipe<NetPayloadSchema>[] clientRequests;
	private static final Logger logger = LoggerFactory.getLogger(SendRoundTripRequest.class);
	
	private long instance;
	private byte[] payloadSource;
	private final int seed;
	private final int maxPayloadSize;
	private final int clockInMs;
	private final int stepSize;
	private final int cyclesPerStep;
	private final int testSteps;
	public final ClientCoordinator ccm;
	
	private int activeCycleCountDown;
	private int activeStepCountDown;
	private int activePayloadSize;
	private long activeNextTrigger;
	
	private ClientConnection activeConnection;
	
	private final CharSequence host;
	private final int port;
	
	public SendRoundTripRequest(GraphManager gm, 
			                    Pipe<NetPayloadSchema>[] clientRequests,
			                    ClientCoordinator ccm,
			                    int seed, 
			                    int clockInMs, //message frequency
			                    long bitsPerSecond, //1_000_000_000  is 1Gbps
			                    int testSteps,
			                    int cyclesPerStep,
			                    CharSequence targetHost,
			                    int targetPort
			                    ) {
		super(gm, NONE, clientRequests);
		
		assert(clockInMs>0);
		assert(bitsPerSecond>0);
		assert(testSteps>=1);
		
		this.host = targetHost;
		this.port = targetPort;
		
		this.clientRequests = clientRequests;
		this.clockInMs = clockInMs;
		this.seed = seed;
		this.maxPayloadSize = (int)((clockInMs * (bitsPerSecond/8L)) / 1000) ;
		this.testSteps = testSteps;
		this.stepSize = this.maxPayloadSize/testSteps;
		this.cyclesPerStep = cyclesPerStep;
		this.ccm = ccm;
				
	}
	
	private void next() {
		if (--activeCycleCountDown<=0) {
			if (--activeStepCountDown<=0) {
				Pipe.publishEOF(clientRequests);
				//all done
				requestShutdown();				
			}
			this.activeCycleCountDown = this.cyclesPerStep;
			this.activePayloadSize += stepSize;
		}
	}
	
	@Override
	public void startup() {

		//build test data to be sent
		Random r = new Random(seed);
	    payloadSource = new byte[maxPayloadSize];
	    r.nextBytes(payloadSource);
		
		//setup first trigger
		this.activePayloadSize = this.stepSize;
		this.activeCycleCountDown = this.cyclesPerStep;
		this.activeStepCountDown = testSteps;
		this.activeNextTrigger = -1;
		
	}
	
	@Override
	public void run() {
		
		long now = System.currentTimeMillis();
		if (now>=activeNextTrigger) {
			
			activeConnection = ccm.connectionId(host, port, sessionId, clientRequests, activeConnection);
			if (null!=activeConnection) {
						
				if (activeNextTrigger>0) {
					//take last time and set next to ensure each leading edge is the same distance apart.
					activeNextTrigger = activeNextTrigger + clockInMs;
				} else {
					activeNextTrigger = now;
				}
				
				Pipe<NetPayloadSchema> pipe = clientRequests[activeConnection.requestPipeLineIdx()];
						
				if (0 == Pipe.contentRemaining(pipe)) {
					//last write was not done, so drop, report and exit.
					logger.warn("WARNING: exited early, unable to write {} bytes every {} ms.",activePayloadSize ,clockInMs);
					//shutdown downstream
					Pipe.publishEOF(clientRequests);
					//all done
					requestShutdown();				
				}
										
				writeMessage(pipe, activeConnection.id);
				
				next();
			}
		}
	}

	void writeMessage(Pipe<NetPayloadSchema> pipe, long connectionId) {
		int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_PLAIN_210);
		Pipe.addLongValue(connectionId, pipe);
		Pipe.addLongValue(System.currentTimeMillis(), pipe); //time left
		Pipe.addLongValue(0, pipe);//no position to release
		DataOutputBlobWriter<NetPayloadSchema> payload = Pipe.openOutputStream(pipe);
		//
		//  <|°_°|>
		//
		//write the rate we are sending these from leading edge in MS
		payload.writePackedLong(payload, clockInMs); //fixed value			
		//write time
		payload.writePackedLong(payload, System.nanoTime());			
		//write instance
		payload.writePackedLong(payload, instance);
		//write remaining for payload (only send the exact size, so subtract our header data)
		int remainingToWrite = activePayloadSize-payload.length();
		payload.write(payloadSource, 0, remainingToWrite);
		////
		////			
		DataOutputBlobWriter.closeLowLevelField(payload);
		Pipe.confirmLowLevelWrite(pipe);
		Pipe.publishWrites(pipe);
	}
	
}
