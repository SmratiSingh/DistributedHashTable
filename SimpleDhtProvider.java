package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;

import static android.content.Context.TELEPHONY_SERVICE;
public class SimpleDhtProvider extends ContentProvider {
    private static Hashtable<String, String> DHT;
    private static Hashtable<String , String> queryTable = new Hashtable<String , String>();
    private static String myPort = null;
    private static String myId = null;
    private String nextPort = null;
    private String nextId = null;
    private String previousPort = null;
    private String predecessorId = null;
    private String endNode;
    private Hashtable<Integer, Integer> nodeTable = new Hashtable<Integer, Integer>();
    private Hashtable<String, Integer> nodeHashtable = new Hashtable<String, Integer>();
    private Hashtable<String, String> nodePortTable = new Hashtable<String, String>();
    private ArrayList<String> nodes = new ArrayList<String>();
    private String[] AVD = new String[]{"5554","5556","5558","5560","5562"};
    private String[] PORTS = new String[]{"11108","11112","11116","11120","11124"};

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equalsIgnoreCase("@"))
            DHT.clear();
        else if(selection.equalsIgnoreCase("*"))
        {
            DHT.clear();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(8),selection,nextPort, myPort);
        }
        else
        if(DHT.containsKey(selection))
            DHT.remove(selection);
        else
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(8),selection,nextPort,myPort);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            if (checkHash(genHash((String)values.get("key"))))
            {
                if(DHT.containsKey(values.get("key")))
                    DHT.remove(values.get("key"));
                DHT.put(values.get("key").toString(), values.get("value").toString());
            }
            else
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(4), 4+";"+values.get("key")+"-"+values.get("value"), nextPort, myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        DHT = new Hashtable<String, String>();
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        nextPort = myPort;     nextId = myId;
        previousPort = myPort;   predecessorId = myId;
        try
        {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, new ServerSocket(10000));
            myId = genHash(portStr);
            for (int i=0;i<PORTS.length;i++)
                nodeHashtable.put(genHash(AVD[i]),Integer.parseInt(PORTS[i]));
            for (int i=0;i<PORTS.length;i++)
                nodeTable.put(Integer.parseInt(PORTS[i]),Integer.parseInt(AVD[i]));
            nextId = genHash(portStr);
            nextPort = myPort;
            predecessorId = genHash(portStr);
            previousPort = myPort;
            endNode = nextId;
            nodes.add(myId);
            if(Integer.parseInt(myPort) != Integer.parseInt(PORTS[0]))
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(1), myPort);
        }catch (IOException ie)
        {
            return false;
        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});
        if(selection.equalsIgnoreCase("@"))
        {
            for(String s: DHT.keySet())
            {
                matrixCursor.moveToFirst();
                matrixCursor.addRow(new Object[]{s, DHT.get(s)});
            }
        }
        else if(selection.equalsIgnoreCase("*"))
        {
            matrixCursor.moveToFirst();
            for(String s: DHT.keySet())
                matrixCursor.addRow(new Object[]{s, DHT.get(s)});
            if(!myPort.equalsIgnoreCase(nextPort))
            {
                for(String s: getFeedback(selection).split(":"))
                {
                    if(!s.contains("-"))
                        continue;
                    matrixCursor.addRow(new Object[]{s.split("-")[0], s.split("-")[1]});
                }
                queryTable.remove(selection);
            }
        }
        else
        {
            matrixCursor.moveToFirst();
            String val = DHT.get(selection);
            if(val != null)
            {
                matrixCursor.addRow(new Object[]{selection, val});
                return matrixCursor;
            }
            else if(!nextPort.equalsIgnoreCase(myPort) && val == null)
            {
                for(String s: getFeedback(selection).split(";"))
                    matrixCursor.addRow(new Object[]{s.split("-")[0],s.split("-")[1]});
                queryTable.remove(selection);
                return matrixCursor;
            }
        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean checkHash(String hash)
    {
        if(myId.equalsIgnoreCase(nextId) && myId.equalsIgnoreCase(predecessorId))
            return true;
        else if(myId.compareTo(predecessorId) > 0 && hash.compareTo(myId) <=0 && hash.compareTo(predecessorId) > 0)
            return true;
        else if(myId.compareTo(predecessorId) < 0 && (hash.compareTo(predecessorId) > 0 || hash.compareTo(myId)<=0))
            return true;
        return false;
    }

    private String getFeedback(String... selection)
    {
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}), Integer.parseInt(nextPort));
            socket.setTcpNoDelay(true);
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());
            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
            outputStream.writeUTF(5+";"+myPort+";"+selection[0]);
            outputStream.flush();
            String res;
            do {
                res = inputStream.readUTF();
            }while(res.equalsIgnoreCase("NACK"));
            do
            {
                Thread.sleep(150);
            }while(!queryTable.containsKey(selection[0]));
            if(inputStream != null)
                inputStream.close();
            if(outputStream != null)
                outputStream.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return queryTable.get(selection[0]);
    }

    private class ClientTask extends AsyncTask<String, Void , Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String remotePort = null;
            String req = null;
            Socket socket = null;
            int msgType = Integer.parseInt(msgs[0]);
            if(msgType == 1)
            {
                remotePort = Integer.toString(11108);
                req = Integer.toString(1)+";"+myPort+";"+myId;
            }
            else if(msgType == 3 || msgType == 7)
            {
                req = msgs[1];
                remotePort = msgs[2];
            }
            else if(msgType == 4)
            {
                req = msgs[1];
                remotePort = msgs[2];
            }
            else if(msgType == 5)
            {
                req = 5+";"+msgs[2]+";"+msgs[3]+";"+msgs[1];
                remotePort = nextPort;
            }
            else if(msgType == 6)
            {
                req = 6+";"+msgs[2]+";"+msgs[3]+";"+msgs[1];
                remotePort = msgs[2];
            }
            else if(msgType == 8)
            {
                req = msgs[1];
                remotePort = msgs[2];
            }
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                socket.setTcpNoDelay(true);
                DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF(req);
                outputStream.flush();
                String reply = "NACK";
                do
                {
                    reply = inputStream.readUTF();
                }while(reply.equalsIgnoreCase("NACK"));
                if(msgType == 1)
                {
                    if(!reply.equalsIgnoreCase("NACK"))
                    {
                        previousPort = reply.split(";")[1];
                        nextPort = reply.split(";")[2];
                        predecessorId = genHash(Integer.toString(nodeTable.get(Integer.parseInt(previousPort))));
                        nextId = genHash(Integer.toString(nodeTable.get(Integer.parseInt(nextPort))));
                    }
                }
                if(outputStream!= null)
                {
                    outputStream.close();
                }

                if(inputStream!= null)
                {
                    inputStream.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                try{
                    socket.close();
                }catch(IOException ie){
                    ie.printStackTrace();
                }
            }
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Socket socket = null;
            do
            {
                try {
                    socket = sockets[0].accept();
                    socket.setTcpNoDelay(true);
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    String string = null;
                    String res = null;
                    while ((string = inputStream.readUTF())!=null) {
                        if(Integer.parseInt(string.split(";")[0]) == 1)
                        {
                            nodes.add(string.split(";")[2]);
                            Collections.sort(nodes);
                            int cur = nodes.indexOf(string.split(";")[2]);
                            endNode = nodes.get(nodes.size() - 1);
                            int previous = (cur -1)% nodes.size();
                            int next = (cur +1)% nodes.size();
                            if (previous < 0)
                                previous = nodes.size() -1;
                            if (next < 0)
                                next = nodes.size() -1;
                            res = 2 + ";" + nodeHashtable.get(nodes.get(previous)) + ";"+nodeHashtable.get(nodes.get(next));
                            outputStream.writeUTF(res);
                            outputStream.flush();
                            publishProgress(Integer.toString(3),
                                    3+";S;"+string.split(";")[1] ,Integer.toString(nodeHashtable.get(nodes.get(previous))),
                                    3+";P;"+string.split(";")[1] ,Integer.toString(nodeHashtable.get(nodes.get(next))));
                        }
                        else if(Integer.parseInt(string.split(";")[0]) == 3)
                        {
                            outputStream.writeUTF("ACK");
                            outputStream.flush();
                            if(string.split(";")[1].equalsIgnoreCase("S"))
                            {
                                nextPort = Integer.toString(Integer.parseInt(string.split(";")[2]));
                                nextId = genHash(Integer.toString(nodeTable.get(Integer.parseInt(string.split(";")[2]))));
                            }
                            else if (string.split(";")[1].equalsIgnoreCase("P"))
                            {
                                String oldPredecessorId = predecessorId;
                                ArrayList<String> keys = new ArrayList<String>();
                                ArrayList<String> values = new ArrayList<String>();
                                predecessorId = genHash(Integer.toString(nodeTable.get(Integer.parseInt(string.split(";")[2]))));
                                previousPort = Integer.toString(Integer.parseInt(string.split(";")[2]));
                                if(!DHT.isEmpty())
                                {
                                    for(String s: DHT.keySet())
                                    {
                                        if(s.compareTo(predecessorId) <= 0)
                                        {
                                            values.add(DHT.get(s));
                                            keys.add(s);
                                        }
                                    }
                                    for(String s:keys)
                                        DHT.remove(s);
                                    String updateReq = 7+";"+values.size()+";";
                                    for(int i=0; i < values.size(); i++)
                                        updateReq = updateReq + keys.get(i)+ "-" + values.get(i)+":";
                                    publishProgress(Integer.toString(7),updateReq.substring(0, updateReq.length()-1), nodePortTable.get(oldPredecessorId));
                                }
                            }
                        }
                        else if (Integer.parseInt(string.split(";")[0]) == 7)
                        {
                            outputStream.writeUTF("ACK");
                            outputStream.flush();
                            for(String s: string.split(";")[2].split(":"))
                                DHT.put(s.split("-")[0], s.split("-")[1]);
                        }
                        else if (Integer.parseInt(string.split(";")[0]) == 4)
                        {
                            outputStream.writeUTF("ACK");
                            outputStream.flush();
                            if(checkHash(genHash(string.split(";")[1].split("-")[0])))
                                DHT.put(string.split(";")[1].split("-")[0], string.split(";")[1].split("-")[1]);
                            else
                                publishProgress(Integer.toString(4),string, nextPort, myPort);
                        }
                        else if(Integer.parseInt(string.split(";")[0]) == 6)
                        {
                            outputStream.writeUTF("ACK");
                            outputStream.flush();
                            if(myPort.equalsIgnoreCase(string.split(";")[1]))
                                queryTable.put(string.split(";")[2], string.split(";")[3]);
                        }
                        else if(Integer.parseInt(string.split(";")[0]) == 5)
                        {
                            outputStream.writeUTF("ACK");
                            outputStream.flush();
                            String resp = null;
                            if (string.split(";").length > 3)
                                resp = string.split(";")[3];
                            boolean req = false;
                            if(string.split(";")[2].equalsIgnoreCase("*"))
                            {
                                resp = resp + ":";
                                req = true;
                                for(String s: DHT.keySet())
                                    resp = resp + s + "-" +DHT.get(s)+":";
                                resp = resp.substring(0, resp.length()-1);
                            }
                            else
                            {
                                if(DHT.get(string.split(";")[2]) != null)
                                    resp = string.split(";")[2] + "-" + DHT.get(string.split(";")[2]);
                                else
                                    req = true;
                            }
                            if(string.split(";")[1].equalsIgnoreCase(nextPort) && req)
                                req = false;
                            if(req)
                                publishProgress(Integer.toString(5), resp, string.split(";")[1], string.split(";")[2], myPort);
                            else
                                publishProgress(Integer.toString(6), resp, string.split(";")[1], string.split(";")[2], myPort);
                        }
                        else if (Integer.parseInt(string.split(";")[0]) == 8)
                        {
                            outputStream.writeUTF("ACK");
                            boolean Fwd = false;
                            if(string.split(";")[1].equalsIgnoreCase("*"))
                            {
                                DHT.clear();
                                Fwd = true;
                            }
                            else
                            {
                                if(DHT.containsKey(string.split(";")[1]))
                                    DHT.remove(string.split(";")[1]);
                                else
                                    Fwd = true;

                            }
                            if(Fwd)
                                publishProgress(Integer.toString(8), string.split(";")[1], nextPort, myPort);
                        }
                    }
                    if (outputStream != null) {outputStream.close();}
                    if (inputStream != null) {inputStream.close();}

                }catch(EOFException e){
                    e.printStackTrace();
                }catch(Exception e)
                {
                    e.printStackTrace();
                }
                try{
                    socket.close();
                }catch(IOException e)
                {
                    e.printStackTrace();
                }
            }while(socket.isConnected());
            return null;
        }
        protected void onProgressUpdate(String... values) {
            if(Integer.parseInt(values[0]) == 1)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(1), myPort);
            else if(Integer.parseInt(values[0]) == 3)
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(3), values[1], values[2], myPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,   Integer.toString(3),values[3], values[4], myPort);
            }
            else if(Integer.parseInt(values[0]) == 7)
            {
                if(!DHT.isEmpty())
                {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(7),values[1], values[2], myPort);
                }
            }
            else if(Integer.parseInt(values[0]) == 4)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(4), values[1] , values[2], myPort);
            else if(Integer.parseInt(values[0]) == 5)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(5), values[1] , values[2], values[3], myPort);
            else if(Integer.parseInt(values[0]) == 6)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(6), values[1] , values[2], values[3], myPort);
            else if(Integer.parseInt(values[0]) == 8)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  Integer.toString(8), values[1] , values[2], myPort);
            return;
        }
    }
}
