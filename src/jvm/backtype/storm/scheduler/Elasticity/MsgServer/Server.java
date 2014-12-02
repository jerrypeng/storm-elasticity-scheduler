package backtype.storm.scheduler.Elasticity.MsgServer;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Elasticity.ElasticityScheduler;

public class Server implements Runnable{

    protected int          serverPort   = 8080;
    protected ServerSocket serverSocket = null;
    protected boolean      isStopped    = false;
    protected Thread       runningThread= null;
    protected Queue<String> serverMsgQueue = null;
    private static final Logger LOG = LoggerFactory
			.getLogger(Server.class);

    public Server(int port, Queue<String> msgQueue){
        this.serverPort = port;
        this.serverMsgQueue = msgQueue;
    }

    public void run(){
        synchronized(this){
            this.runningThread = Thread.currentThread();
        }
        openServerSocket();
        while(! isStopped()){
            Socket clientSocket = null;
            try {
                clientSocket = this.serverSocket.accept();
                LOG.info("Connect to "+clientSocket.getInetAddress()+":"+clientSocket.getPort());
            } catch (IOException e) {
                if(isStopped()) {
                    LOG.info("Server Stopped.") ;
                    return;
                }
                throw new RuntimeException(
                    "Error accepting client connection", e);
            }
            new Thread(
                new WorkerRunnable(
                    clientSocket, this.serverMsgQueue)
            ).start();
        }
       LOG.info("Server Stopped.") ;
    }


    private synchronized boolean isStopped() {
        return this.isStopped;
    }

    public synchronized void stop(){
        this.isStopped = true;
        try {
            this.serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }

    private void openServerSocket() {
        try {
            this.serverSocket = new ServerSocket(this.serverPort);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port "+this.serverPort, e);
        }
    }

}