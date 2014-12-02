package backtype.storm.scheduler.Elasticity.MsgServer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Queue;

/**

 */
public class WorkerRunnable implements Runnable{

    protected Socket clientSocket = null;
    protected Queue<String> msgQueue = null;

    public WorkerRunnable(Socket clientSocket, Queue<String> msgQueue) {
    	this.msgQueue = msgQueue;
        this.clientSocket = clientSocket;
    }

    public void run() {
        try {
            InputStream input  = clientSocket.getInputStream();
            Integer data =null;
            String msg="";
            while((data = input.read()) != -1) {
            	char ch = (char) data.intValue();
            	msg+=ch;
            }
            //System.out.println(msg);
            this.msgQueue.add(msg);
           
        } catch (IOException e) {
            //report exception somewhere.
            e.printStackTrace();
        }
    }
}