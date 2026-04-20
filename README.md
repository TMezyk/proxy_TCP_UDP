# Networking project: proxy

About:

A Java-based proxy that bridges UDP and TCP clients to multiple backend servers, supporting distributed key-value operations.
Clients connect to the proxy, which forwards requests to a suitable server or another proxy closer to the target server.
Each key is unique in the network.

Features:
- UDP/TCP protocol translation between client and server
- network shutdown propagation across nodes
- anti-loop safeguards
- network scalability

How to run:

1. Compile the classes with javac using Java 21
2. Run via command line interface
3. Syntax for server classes: -port [port] -key [key] (a String) -value [value] (an Integer)
4. Syntax for client classes: -address [address] -port [port] -command (one of: GET NAMES, GET VALUE [key], SET [key] [value], QUIT)
5. Syntax for Proxy class: -port [port] -server [address] [port] (multiple servers can be specified by providing -server again)

Notes:

The following classes: TCPClient, UDPClient, TCPServer, UDPServer have been pre-provided and thus are not of my authorship. The goal was solely to create the proxy class.