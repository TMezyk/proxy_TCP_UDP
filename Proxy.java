import java.io.*;
import java.net.*;
import java.util.*;

public class Proxy {

    private static final HashMap<String,HashSet<String>> proxies = new HashMap<String,HashSet<String>>();
    private static final HashMap<String, String> keyholdersUDP = new HashMap<String,String>();
    private static final HashMap<String, String> keyholdersTCP = new HashMap<String,String>();
    private static final HashSet<String> allKeys = new HashSet<String>();
    private static boolean working = true;

    private static String myId = "";

    public static void main(String[] args) throws IOException {

        int port;
        ServerSocket ss;
        DatagramSocket ds;

        ArrayList<String> servers = new ArrayList<String>();

        System.out.println("--Startup");
        if(args.length > 0) {

            if(!args[0].equals("-port")) throw new IllegalArgumentException("no port specified");

            try {
                port = Integer.parseInt(args[1]);
                if (port < 1 || port > 65535) throw new NumberFormatException("port out of range");
            } catch(Exception e) { throw new IllegalArgumentException("invalid port specified"); }

            if (args.length-2 == 0) { throw new IllegalArgumentException("no servers specified"); }
            else if ((args.length-2) % 3 != 0) { throw new IllegalArgumentException("invalid server specification"); }
            for (int i = 2; i < args.length-2; i+=3) {
                if (!args[i].equals("-server")) throw new IllegalArgumentException("valid server specification expected");
                try {
                    servers.add(args[i + 1] + ":" + args[i + 2]);
                } catch(Exception e) { throw new IllegalArgumentException("invalid server specification"); }
            }


        } else throw new IllegalArgumentException("no argument specified");

        try {
            ss = new ServerSocket(port);
            ds = new DatagramSocket(port);
            ss.setSoTimeout(500);
            ds.setSoTimeout(500);
        } catch(Exception e) { throw new IllegalArgumentException("could not listen on port " + port); }

        try {
            myId = InetAddress.getLocalHost().getHostAddress() + ":" + port;
        } catch(Exception e) {
            myId = "127.0.0.1:" + port;
        }

        System.out.println("--Network mapping");
        for(String server : servers) {

            try {
                String response = send("GET NAMES", server, true, true);
                response = response.trim();
                String[] responseParts = response.split("\\s+");
                if (responseParts.length < 3 || !responseParts[0].startsWith("OK")) throw new IOException();

                int n = Integer.parseInt(responseParts[1]);

                if (n > 1) {
                    proxies.put(server, new HashSet<String>());
                    for (int i = 2; i < responseParts.length; i++) {
                        proxies.get(server).add(responseParts[i]);
                        allKeys.add(responseParts[i]);
                    }
                    System.out.println("Identified server " + server + " as fellow proxy");
                } else {
                    String proxyTest = send("PING", server, true, true).trim();
                    if (proxyTest.equals("PONG")) {
                        proxies.put(server, new HashSet<String>());
                        proxies.get(server).add(responseParts[2]);
                        allKeys.add(responseParts[2]);
                        System.out.println("Identified server " + server + " as fellow proxy");
                    }
                    else {
                        keyholdersUDP.put(server, responseParts[2]);
                        allKeys.add(responseParts[2]);
                        System.out.println("Identified server " + server + " as UDP keyholder");
                    }
                }

            } catch(SocketTimeoutException e) {
                try {
                    System.out.println("Timeout on " + server + ". Needs verification");

                    String response = send("GET NAMES", server, false, true);
                    if (response == null || response.isEmpty()) throw new IOException();

                    response = response.trim();
                    String[] responseParts = response.split("\\s+");
                    if (responseParts.length < 3 || !responseParts[0].startsWith("OK")) throw new IOException();

                    keyholdersTCP.put(server, responseParts[2]);
                    allKeys.add(responseParts[2]);
                    System.out.println("Identified server " + server + " as TCP keyholder");

                } catch (Exception e1) {
                    System.out.println(server + " is either inactive or not a keyholder");
                }
            } catch(Exception e) {
                System.out.println(server + " is unreachable");
            }

        }
        servers.clear();

        System.out.println("--Network mapping finished");
        String message = "KEYS ";
        String messageBuilder = allKeys.toString();
        message += messageBuilder.substring(1, messageBuilder.length() - 1).replaceAll(",", "");
        message += " PATH " + myId;
        for (String proxy : proxies.keySet()) { send(message, proxy, true, false); }

        while(working) {

            try {
                Socket client = ss.accept();

                InputStream is = client.getInputStream();
                OutputStream os = client.getOutputStream();
                InputStreamReader isr = new InputStreamReader(is);
                OutputStreamWriter osw = new OutputStreamWriter(os);
                BufferedWriter bw = new BufferedWriter(osw);
                BufferedReader br = new BufferedReader(isr);
                client.setSoTimeout(500);

                String line = br.readLine();
                String response = makeResponse(line);
                if (!response.isEmpty()) {
                    bw.write(response + "\r\n");
                    bw.flush();
                }
                client.close();

            } catch(Exception ignored) {}
            try {
                byte[] buffer = new byte[2048];
                DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                ds.receive(incoming);

                String line = new String(incoming.getData(), 0, incoming.getLength());

                String response = makeResponse(line);
                if (!response.isEmpty()) {
                    byte[] data = (response + "\r\n").getBytes();
                    DatagramPacket packet = new DatagramPacket(data, data.length, incoming.getAddress(), incoming.getPort());
                    ds.send(packet);
                }

            } catch(Exception ignored) {}

        }

        ss.close();
        ds.close();

    }

    private static String send(String message, String server, boolean udp, boolean expectResponse) throws SocketTimeoutException, IOException {

        String[] addressPort = server.split(":");

        if (udp) {

            DatagramSocket out = new DatagramSocket();
            out.setSoTimeout(1000);

            byte[] data = (message + "\r\n").getBytes();
            DatagramPacket packet = new DatagramPacket(data, data.length, InetAddress.getByName(addressPort[0]), Integer.parseInt(addressPort[1]));
            out.send(packet);

            if (expectResponse) {
                byte[] buffer = new byte[2048];
                DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                out.receive(incoming);
                String response = new String(incoming.getData(), 0, incoming.getLength());
                out.close();
                return response;
            }
            out.close();
            return "";
        }
        Socket socket = new Socket(addressPort[0], Integer.parseInt(addressPort[1]));
        InputStream is = socket.getInputStream();
        OutputStream os = socket.getOutputStream();
        InputStreamReader isr = new InputStreamReader(is);
        OutputStreamWriter osw = new OutputStreamWriter(os);
        BufferedWriter bw = new BufferedWriter(osw);
        BufferedReader br = new BufferedReader(isr);
        socket.setSoTimeout(1000);

        bw.write(message + "\r\n");
        bw.flush();

        if (expectResponse) {
            String response = br.readLine();
            socket.close();
            return response;
        }

        socket.close();
        return "";
    }

    private static String[] findKey(String key) {

        for(Map.Entry<String,String> entry : keyholdersUDP.entrySet()) if (entry.getValue().equals(key)) return new String[]{entry.getKey(),"UDP"};
        for(Map.Entry<String,String> entry : keyholdersTCP.entrySet()) if (entry.getValue().equals(key)) return new String[]{entry.getKey(),"TCP"};
        for(Map.Entry<String,HashSet<String>> entry : proxies.entrySet()) {
            for(String s : entry.getValue()) if (s.equals(key)) return new String[]{entry.getKey(),"UDP"};
        }

        return null;
    }

    private static String extractCommand(String command) {

        if (command.contains("PATH")) return command.substring(0, command.indexOf(" PATH ")).trim();
        return command.trim();
    }

    private static String getNetPath(String[] parts) {
        for (int i = 0; i < parts.length - 1; i++) {if (parts[i].equals("PATH")) return parts[i+1];}
        return "";
    }

    private static boolean isInPath(String netpath, String id) {
        if (netpath == null || netpath.isEmpty()) return false;
        String[] pathParts = netpath.split(",");
        for (String proxy : pathParts) if (proxy.equals(id)) return true;
        return false;
    }

    private static String addPath(String base, String netpath) {
        return base + " PATH " + netpath;
    }

    private static String makeResponse(String command) {
        try {
            command = command.trim();

            if (command.startsWith("GET NAMES")) return handleGetNames();
            if (command.startsWith("GET VALUE")) return handleGetValue(command);
            if (command.startsWith("SET")) return handleSet(command);
            if (command.startsWith("QUIT")) return handleQuit();
            if (command.startsWith("KEYS")) return handleKeys(command);

            else if (command.startsWith("PING")) {
                System.out.println("Pinged. Responded with PONG");
                return "PONG";
            }

        } catch (Exception e) {
            System.out.println(e + " Error with command " + command + ". Responded with NA");
            return "NA";
        }
        System.out.println("Unknown command " + command);
        return "NA";
    }

    private static String handleGetNames() {
        String response = "OK " + allKeys.size() + " ";
        String keys = allKeys.toString();
        response += allKeys.toString().substring(1, keys.length() - 1).replaceAll(",", "");
        System.out.println("Command GET NAMES. Responded with " + response);
        return response;
    }

    private static String handleGetValue(String command) throws IOException {
        String[] parts = command.split("\\s+");
        if (parts.length < 3) return "NA";

        String key = parts[2];
        String[] keyholder = findKey(key);
        if (keyholder == null) {
            System.out.println("Command " + command + ". No key found. Responded with NA");
            return "NA";
        }

        if (proxies.containsKey(keyholder[0])) {
            String netpath = getNetPath(parts);

            if (netpath.isEmpty()) {
                netpath = myId;
            } else {
                if (isInPath(netpath, myId)) return "";
                netpath = netpath + "," + myId;
            }

            System.out.println("Command " + command + ". Authoritative server is " + keyholder[1] + " " + keyholder[0]);
            String response = send(addPath(extractCommand(command), netpath), keyholder[0], true, true);
            if (response == null || response.isEmpty()) return "NA";
            response = response.trim();
            System.out.println("Command " + command + ". Responded with " + response);
            return response;
        }

        System.out.println("Command " + command + ". Authoritative server is " + keyholder[1] + " " + keyholder[0]);
        String response = "OK ";
        String[] responseBuilder;
        if (keyholder[1].equals("UDP"))
            responseBuilder = send(extractCommand(command), keyholder[0], true, true).trim().split("\\s+");
        else responseBuilder = send(extractCommand(command), keyholder[0], false, true).trim().split("\\s+");
        response += responseBuilder[1];
        System.out.println("Command " + command + ". Responded with " + response);
        return response;
    }

    private static String handleSet(String command) throws IOException {
        String[] parts = command.split("\\s+");
        if (parts.length < 3) return "NA";

        String key = parts[1];
        String[] keyholder = findKey(key);
        if (keyholder == null) {
            System.out.println("Command " + command + ". No key found. Responded with NA");
            return "NA";
        }

        if (proxies.containsKey(keyholder[0])) {
            String netpath = getNetPath(parts);

            if (netpath.isEmpty()) {
                netpath = myId;
            } else {
                if (isInPath(netpath, myId)) return "";
                netpath = netpath + "," + myId;
            }

            System.out.println("Command " + command + ". Authoritative server is " + keyholder[1] + " " + keyholder[0]);
            String response = send(addPath(extractCommand(command), netpath), keyholder[0], true, true);
            if (response == null || response.isEmpty()) return "NA";
            response = response.trim();
            System.out.println("Command " + command + ". Responded with " + response);
            return response;
        }

        System.out.println("Command " + command + ". Authoritative server is " + keyholder[1] + " " + keyholder[0]);
        String response = "";
        if (keyholder[1].equals("UDP"))
            response = send(extractCommand(command), keyholder[0], true, true);
        else response = send(extractCommand(command), keyholder[0], false, true);
        if (response == null) return "NA";
        response = response.trim();
        System.out.println("Command " + command + ". Responded with " + response);
        return response.isEmpty() ? "NA" : response;
    }

    private static String handleQuit() {
        System.out.println("Command QUIT. Quitting");
        for (String server : keyholdersTCP.keySet()) try { send("QUIT", server, false, false); } catch (Exception ignored) {}
        for (String server : keyholdersUDP.keySet()) try { send("QUIT", server, true, false); } catch (Exception ignored) {}
        for (String server : proxies.keySet()) try { send("QUIT", server, true, false); } catch (Exception ignored) {}
        working = false;

        return "";
    }

    private static String handleKeys(String command) throws IOException {
        System.out.println("Command " + command + ". Updating keys");
        String[] parts = command.split("\\s+");

        String netpath = getNetPath(parts);
        String[] pathParts = netpath.split(",");

        HashSet<String> keys = new HashSet<>();

        int i = 1;
        String key = parts[i];
        while (!key.equals("PATH")){
            if (!allKeys.contains(key)) keys.add(key);
            allKeys.add(key);
            i++;
            key = parts[i];
        }
        if (proxies.containsKey(pathParts[pathParts.length-1])) { proxies.get(pathParts[pathParts.length-1]).addAll(keys); }
        else  { proxies.put(pathParts[pathParts.length-1], keys); }

        if (!isInPath(netpath, myId)) {
            System.out.println("Command " + command + ". Will send key update request to all known proxies");
            String message = "KEYS ";
            String messageBuilder = allKeys.toString();
            message += messageBuilder.substring(1, messageBuilder.length() - 1).replaceAll(",", "");
            message += " PATH " + netpath + "," + myId;
            for (String proxy : proxies.keySet()) {
                send(message, proxy, true, false);
            }
        }
        return "";
    }

}