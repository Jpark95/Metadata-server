#!/usr/bin/env python

import sys, os, hashlib, logging, threading, time
logging.basicConfig()
sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# Protocol specific imports
from metadataServer import MetadataServerService
from shared.ttypes import *
from blockServer import  *
from blockServer.ttypes import *

def getBlockServerPort(config_path):
    # This function reads config file and gets the port for block server

    print "Checking validity of the config path"
    if not os.path.exists(config_path):
        print "ERROR: Config path is invalid"
        exit(1)
    if not os.path.isfile(config_path):
        print "ERROR: Config path is not a file"
        exit(1)

    print "Reading config file"
    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if 'block' in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())

    # Exit if you did not get blockserver information
    print "ERROR: blockserver information not found in config file"
    exit(1)

def getBlockServerSocket(port):
    # This function creates a socket to block server and returns it

    # Make socket
    transport = TSocket.TSocket('localhost', port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = BlockServerService.Client(protocol)

    # Connect!
    print "Connecting to block server on port", port
    try:
        transport.open()
    except Exception as e:
        print "ERROR: Exception while connecting to block server on port:", port
        print e
        exit(1)

    return client


def getNumOfPorts(config_path):
    if not os.path.exists(config_path):
        print "ERROR"
        exit(1)
    if not os.path.isfile(config_path):
        print "ERROR"
        exit(1)
    with open(config_path, 'r') as conffile:
	lines = conffile.readlines()
        for line in lines:
            if line in lines:
                if 'M' in line:
		    return int(line.split()[1].lstrip().rstrip())
	print "ERROR"
	exit(1)

def getMetaServerPort(config_path, my_id):
    # This function reads config file and gets the port for block server
    if not os.path.exists(config_path):
        print "ERROR"
        exit(1)
    if not os.path.isfile(config_path):
        print "ERROR"
        exit(1)
    myPort = "metadata" + str(my_id)
    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if myPort in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())
    # Exit if you did not get metadata server information
    print "ERROR"
    exit(1)

def getMetaServerSocket(port):
    transport = TSocket.TSocket('localhost', port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = MetadataServerService.Client(protocol)
    try:
        transport.open()
    except Exception as e:
        client = None
    return client


class MetadataServerHandler():

    def __init__(self, config_path, my_id):
        # Initialize block
	self.my_id = my_id
	self.config_path = config_path
	self.numPorts = getNumOfPorts(config_path)
	self.port = self.readServerPort()
	s = getBlockServerPort(config_path)
	self.sock = getBlockServerSocket(s)
	self.hashBlocks = {}
	self.deletedFiles = {}
	self.t = threading.Thread(target = self.gossip)
	self.t.daemon = True
	self.t.start()

    def gossip(self):
	while True:
	    time.sleep(5)
	    i = (int(self.my_id) % self.numPorts) + 1
	    while i != int(self.my_id):
		metaPort = getMetaServerPort(self.config_path, i)
		tempSock = getMetaServerSocket(metaPort)
		if tempSock != None:
		    if len(self.hashBlocks) == 0:
			break
		    '''for fname1 in self.deletedFiles:
			if self.deletedFiles[fname1] > 0:
			    q = self.deletedFiles[fname1]
			    tempSock.deleteFromServer(fname1, q)
			self.deletedFiles.pop(fname1, None)'''
		    for fname in self.hashBlocks:
			print fname
			print "numFiles =", len(self.hashBlocks)
			if self.deletedFiles.has_key(fname) == True:
			    if self.deletedFiles[fname] > 0:
				q = self.deletedFiles[fname]
				tempSock.deleteFromServer(fname, q)
			    self.deletedFiles.pop(fname, None)
			    break
			retFile = tempSock.updateFile(self.hashBlocks[fname])
			if retFile.status == responseType.OK:
			    self.hashBlocks[fname] = retFile
		    break
		i = (i % self.numPorts) + 1
	    
    def updateFile(self, f):
	#if f.filename in self.deletedFiles:
	    #return responseType.ERROR
	if self.hashBlocks.has_key(f.filename):
	    if f.version < self.hashBlocks[f.filename].version:
		return self.hashBlocks[f.filename]
	newF = file()
	self.hashBlocks[f.filename] = f
	newF.status = responseType.ERROR
	return newF

    def getFile(self, filename, v):
        # Function to handle download request from file
	if self.hashBlocks.has_key(filename):
	    if self.hashBlocks[filename].version > v:
	        return self.hashBlocks[filename]
	returnFile = file()
	returnFile.status = responseType.ERROR
	return returnFile

    def storeFile(self, filename):
        # Function to handle upload request
	uResponse = uploadResponse()
	missingHash = []
	if self.hashBlocks.has_key(filename.filename):
	    if filename.version < self.hashBlocks[filename.filename].version:
		uResponse.status = uploadResponseType.FILE_ALREADY_PRESENT
		return uResponse
	
	# LOOP through each block
	for hString in filename.hashList:
	    hBlock = self.sock.hasBlock(hString)
	    if hBlock == False:
		missingHash.append(hString)
	uResponse.hashList = missingHash
	if len(missingHash) == 0:
	    uResponse.status = uploadResponseType.OK
	    #if filename.filename in self.deletedFiles:
	#	self.deletedFiles.remove(filename.filename)
	    self.hashBlocks[filename.filename] = filename
	else:
	    uResponse.status = uploadResponseType.MISSING_BLOCKS
	return uResponse

    def deleteFromServer(self, filename, q):
	if self.hashBlocks.has_key(filename) == True:
	    #self.hashBlocks[filename] = None
	    self.hashBlocks.pop(filename, None)
	    self.deletedFiles[filename] = (q-1)

    def deleteFile(self, filename):
	#print "IN DELETE FILE"
        # Function to handle download request from file
	if self.hashBlocks.has_key(filename) == False:
	    r = response()
	    r.message = responseType.ERROR
	    return r
	self.deletedFiles[filename] = 2
	self.hashBlocks[filename] = None
        self.hashBlocks.pop(filename,None)
	r = response()
	r.message = responseType.OK
	#print len(self.deletedFiles),":", self.deletedFiles[0]
	return r

    def readServerPort(self):
        # Get the server port from the config file.
        # id field will determine which metadata server it is 1, 2 or n
        # Your details will be then either metadata1, metadata2 ... metadatan
        # return the port
	print "Checking validity of config path"
	if not os.path.exists(config_path):
	    print "ERROR: Config path is invalid"
	    exit(1)
	if not os.path.isfile(config_path):
	    print "ERROR: Config path is not a file"
	    exit(1)

	myPort = "metadata" + my_id

	print "Reading config file"
	with open(config_path, 'r') as conffile:
            lines = conffile.readlines()
            for line in lines:
                if myPort in line:
                    # Important to make port as an integer
                    return int(line.split()[1].lstrip().rstrip())

        # Exit if you did not get blockserver information
        print "ERROR: blockserver information not found in config file"
        exit(1)

    # Add other member functions if needed

# Add additional classes and functions here if needed

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Invocation <executable> <config_file> <id>"
        exit(-1)

    config_path = sys.argv[1]
    my_id = sys.argv[2]

    print "Initializing metadata server"
    handler = MetadataServerHandler(config_path, my_id)
    port = handler.readServerPort()
    # Define parameters for thrift server
    processor = MetadataServerService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    # Create a server object
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print "Starting server on port : ", port
    try:
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print "\nExecption / Keyboard interrupt occured: ", e
        exit(0)
