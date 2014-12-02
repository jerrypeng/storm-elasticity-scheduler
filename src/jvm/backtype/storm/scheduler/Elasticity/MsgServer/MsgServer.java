package backtype.storm.scheduler.Elasticity.MsgServer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MsgServer {
	
	Server server;
	Integer port;
	static MsgServer _instance = null;
	public Queue<String> msgQueue = new ConcurrentLinkedQueue<String>();
	
	private MsgServer(Integer port) {
		this.port = port;
		System.out.println("Starting Server on port "+this.port);
		this.server = new Server(this.port, this.msgQueue);
		new Thread(server).start();
	}
	
	public static MsgServer start(Integer port) {
		if(_instance == null) {
			_instance = new MsgServer(port);
		}
		return _instance;
	}
	
	public boolean isRebalance() {
		while(this.msgQueue.isEmpty()!=true) {
			String msg = this.msgQueue.remove();
			if(msg.equals("REBALANCE") == true) {
				return true;
			}
		}
		return false;
	}
}
