package client;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import javax.swing.JOptionPane;

/**
 * Trivial client for the date server.
 */
public class Client {

    /**
     * Runs the client as an application.  First it displays a dialog
     * box asking for the IP address or hostname of a host running
     * the date server, then connects to it and displays the date that
     * it serves.
     */
    public static void main(String[] args) throws IOException {
        
        Socket s = new Socket("localhost", 5001);
        
        BufferedOutputStream bos = new BufferedOutputStream(s.
                getOutputStream());
        OutputStreamWriter osw = new OutputStreamWriter(bos, "US-ASCII");
        osw.write("rebalance");
        osw.flush();
    }
}