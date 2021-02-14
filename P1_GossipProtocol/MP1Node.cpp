/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include "Member.h"
#include <tuple>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 42;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        // Node that wants to join the group, send JOINREQ message to request joining
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    Member* me = (Member*) env;
    MessageHdr* msg = (MessageHdr*) data;
    char* send_addr = (char*) &data[sizeof(MessageHdr)];
    long* heartbeat = (long*) &data[sizeof(MessageHdr)+1+6];

    printf("Msg [%d] [%s]<-[%s], heartbeat [%ld]\n", msg->msgType, me->addr.getAddress().c_str(), this->getAddressString(send_addr).c_str(), *heartbeat);

    switch(msg->msgType){
        case JOINREQ:{
            // Add a new node to the MemberList and send it the current member list

            MemberListEntry member = MemberListEntry(int(send_addr[0]), int(send_addr[4]), *heartbeat, me->heartbeat);
            me->memberList.push_back(member);
            printf("Adding new node [%d] to group\n", member.getid());

            // Get all active nodes in form of a buffer ready for sending
            char *active_nodes;
            size_t buffer_size = this->getActiveMembersBuffer(&active_nodes);

            // Build a message containing all active nodes
            size_t msgsize = sizeof(MessageHdr) + 6 + sizeof(long) + 1 + buffer_size;
            msg = (MessageHdr *) malloc(msgsize * sizeof(char));

            // create JOINREP message: format of data is {struct Address myaddr}
            msg->msgType = JOINREP;
            memcpy((char *)(msg+1), &memberNode->addr.addr, 6);
            memcpy((char *)(msg+1) + 1 + 6, &memberNode->heartbeat, sizeof(long));
            // attach active node buffer
            memcpy((char *)(msg+1) + 1 + 6 + sizeof(long), active_nodes, buffer_size);

            Address recv = Address();
            recv.init();
            recv.addr[0] = send_addr[0];
            recv.addr[4] = send_addr[4];
            emulNet->ENsend(&memberNode->addr, &recv, (char *)msg, msgsize);

            free(active_nodes);
            break;
        }
        case JOINREP:{
            printf("Received join group response!\n");

            size_t offset = sizeof(MessageHdr)+1+6+sizeof(long);
            int* nElements = (int*) &data[offset];
            char* elementsBuffer = (char*) &data[offset+sizeof(int)];
            updateActiveMembers(*nElements, elementsBuffer);
            break;
        }
        default:{
            printf("Unknown message type received!\n");
            break;
        }
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    this->memberNode->heartbeat++;

    long now = this->memberNode->heartbeat;
    long elapsed_time;
    // Using a for loop with iterator
    for(std::vector<MemberListEntry>::iterator it = std::begin(this->memberNode->memberList); it != std::end(this->memberNode->memberList);) {
        elapsed_time = now - it->gettimestamp();

       if(elapsed_time >= TREMOVE){
            printf("T:%ld Time to get rid of node %d\n", now, it->getid());
            it = this->memberNode->memberList.erase(it);
        }
        else{
            it++;
        }
    }
    return;
}

void MP1Node::updateActiveMembers(int nEntries, char *buffer){
    
    long now = this->memberNode->heartbeat;
    size_t element_size = sizeof(int) + sizeof(long);
    int* nodeid;
    long* heartbeat;
    for(int i=0; i < nEntries; i++){
        nodeid = (int*) &buffer[i*element_size];
        heartbeat = (long*) &buffer[i*element_size + sizeof(int)];

        printf("[%d] Process Memberlist update entry for nodeid %d, heartbeat %ld\n", memberNode->addr.addr[0], *nodeid, *heartbeat);
        // If the node is present in the member list, and the heartbeat of the
        // received entry is higher, update the MemberListEntry.
        // If the nodeid is not in the MemberList, we found a new node, add it to the list
        for(std::vector<MemberListEntry>::iterator it = std::begin(this->memberNode->memberList); it != std::end(this->memberNode->memberList);it++) {
            if(*nodeid == it->getid()){
                if(*heartbeat > it->getheartbeat()){
                    it->setheartbeat(*heartbeat);
                    it->settimestamp(now);
                    printf("[%d] Update entry for node [%d]\n", memberNode->addr.addr[0], *nodeid);
                }
                goto nextEntry;
            }
        }
        {
            // Add new node
            printf("[%d] Adding new node [%d] to Members list during update\n", memberNode->addr.addr[0], *nodeid);
            MemberListEntry member = MemberListEntry(*nodeid, 0, *heartbeat, now);
            memberNode->memberList.push_back(member);
        }

        nextEntry: // Jump to this if node was already present in MemberList
        continue;
    }
}


size_t MP1Node::getActiveMembersBuffer(char** buffer){
    std::vector<std::tuple<int, long>> active_nodes = this->getActiveMembers();
    int size = active_nodes.size();
    size_t element_size = sizeof(int) + sizeof(long);

    //TODO: Check how to assign buffer passed as argument
    *buffer = (char *) malloc(element_size * size + sizeof(int));

    memcpy(&((*buffer)[0]), &size, sizeof(int));
    for(int i=0; i < size; i++) {
        memcpy(&((*buffer)[element_size*i + sizeof(int)]), &std::get<0>(active_nodes[i]), sizeof(int));
        memcpy(&((*buffer)[element_size*i + sizeof(int) + sizeof(int)]), &std::get<1>(active_nodes[i]), sizeof(long));
    }
    return element_size * size + sizeof(int);
}

std::vector<std::tuple<int, long>> MP1Node::getActiveMembers()
{
    std::vector<std::tuple<int, long>> active_nodes;
    long now = this->memberNode->heartbeat;
    long elapsed_time;

    for(std::vector<MemberListEntry>::iterator it = std::begin(this->memberNode->memberList); it != std::end(this->memberNode->memberList);it++) {
        elapsed_time = now - it->gettimestamp();

       if(elapsed_time < TFAIL){
            printf("Active node %d\n", it->getid());
            active_nodes.push_back(std::make_tuple(it->getid(), it->getheartbeat()));
        }
    }
    return active_nodes;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

string MP1Node::getAddressString(char* addr){
    	int id = 0;
		short port;
		memcpy(&id, &addr[0], sizeof(int));
		memcpy(&port, &addr[4], sizeof(short));
		return to_string(id) + ":" + to_string(port);
}