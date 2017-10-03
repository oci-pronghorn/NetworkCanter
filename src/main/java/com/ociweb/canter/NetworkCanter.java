package com.ociweb.canter;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.ClientResponseParserFactory;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.ServerFactory;
import com.ociweb.pronghorn.network.ServerPipesConfig;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.util.MainArgs;

public class NetworkCanter  {

	private static final int seed = 1234;
	private static final long rate = 20_000;
	
	public static void main(String[] args) {
		
		String host = MainArgs.getOptArg("host", "-h", args, null);
		if (null==host) {
			host = NetGraphBuilder.bindHost();
		}		
		String portString = MainArgs.getOptArg("port", "-p", args, "8842"); 
		int port = Integer.parseInt(portString);
				
		
		String clockString = MainArgs.getOptArg("clockMs", "-c", args, "100"); 
		int clockMs = Integer.parseInt(clockString);
		
		String bpsString = MainArgs.getOptArg("bps", "-b", args, "1_000_000_000"); 
		int bps = Integer.parseInt(bpsString);
		
		String stepsString = MainArgs.getOptArg("steps", "-s", args, "10"); 
		int steps = Integer.parseInt(stepsString);
		
		String cyclesString = MainArgs.getOptArg("cycles", "-y", args, "10"); 
		int cycles = Integer.parseInt(cyclesString);
			
		
		GraphManager gm = new GraphManager();
		
		buildBounceServer(gm, host, port);

		
		//only run client when the target host is defined
		String targetHost = MainArgs.getOptArg("targetHost", "-th", args, null); 
		if (null != targetHost) {
			String targetPortString = MainArgs.getOptArg("targetPort", "-tp", args, "8842"); 
			int targetPort = Integer.parseInt(targetPortString);
			buildTestClient(gm, clockMs, bps, steps, cycles, targetHost, targetPort);
		}
		
		gm.enableTelemetry(8089);
		
		StageScheduler.defaultScheduler(gm).startup();
		
	}

	private static void buildTestClient(GraphManager gm, int clockMs, int bps, int steps, int cycles,
			final String targetHost, final int targetPort) {
		final int conBits=2;
		final int maxResp=2;
		
		ClientCoordinator ccm = new ClientCoordinator(conBits, maxResp, false);
				
		ccm.setStageNotaProcessor((g,s)->{
			GraphManager.addNota(g, GraphManager.SCHEDULE_RATE, rate, s);
		});
		
		ClientResponseParserFactory clientFactory = new ClientResponseParserFactory() {
			@Override
			public void buildParser(GraphManager gm, 
					ClientCoordinator ccm, 
					Pipe<NetPayloadSchema>[] response,
					Pipe<ReleaseSchema> ackRelease) {
				GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, 				
						new ConsumeTotalTimeStage(gm, ccm, response, ackRelease)
				);
			}
		}; 
		
		final int minimumFragmentsOnRing = 10;
		final int maximumLenghOfVariableLengthFields = 10_000;
		
		
		Pipe<NetPayloadSchema>[] clientRequests = new Pipe[]{NetPayloadSchema.instance.newPipe(
				minimumFragmentsOnRing, maximumLenghOfVariableLengthFields)};
		
		GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, rate, 	
				new SendRoundTripRequest(gm, clientRequests, 
						                 ccm, seed, clockMs, bps, steps, cycles,
						                 targetHost, targetPort)
		);
		  
		NetGraphBuilder.buildSimpleClientGraph(gm, ccm, clientFactory, clientRequests);
	}

	private static void buildBounceServer(GraphManager gm, String host, int port) {
		ServerPipesConfig serverConfig = new ServerPipesConfig(false, false);
		ServerCoordinator coordinator = new ServerCoordinator(false, host, port, serverConfig);
		
		ServerFactory serverFactory = new ServerFactory() {

			@Override
			public void buildServer(GraphManager graphManager, 
					ServerCoordinator coordinator,
					Pipe<ReleaseSchema>[] releaseAfterParse, 
					Pipe<NetPayloadSchema>[] receivedFromNet,
					Pipe<NetPayloadSchema>[] sendingToNet) {	
				
				GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, rate, 				
						new BounceStage(graphManager, receivedFromNet, releaseAfterParse, sendingToNet)			
				);
			}			
		};
		NetGraphBuilder.buildServerGraph(gm, coordinator, serverConfig, rate, serverFactory);
	}
	 
}
