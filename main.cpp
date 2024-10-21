#include <iostream>
#include <atomic>  // Include atomic for the flag

#include <string>
#include <vector>
#include <set>
#include <map>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fstream>
#include <getopt.h>
#include <sstream>
#include <algorithm>

#define PORT 5000  // Base port number for TCP connections

enum MessageType {
    JOIN,
    REQ,
    OK,
    NEWVIEW,
    NEWLEADER,
    QUE,
    RESP,
    COMPLETE_PENDING
};

enum OperationType {
    ADD,
    DEL,
    PENDING,
    NOTHING
};

struct RequestID {
    int leader_id;
    int nonce; // 8-digit integer nonce

    bool operator<(const RequestID& other) const {
        if (leader_id != other.leader_id)
            return leader_id < other.leader_id;
        return nonce < other.nonce;
    }
};



struct Message {
    MessageType type;
    RequestID request_id;
    int view_id;
    OperationType operation;
    int peer_id;          // The sender
    int target_peer_id;   // The target of the operation
    std::vector<int> memb_list;
    int leader_id;
    std::string text;
};

struct PendingOperation {
    bool exists;
    OperationType operation;
    int peer_id; // The peer being added/removed in the pending operation
};

class Peer {
public:
    int id;
    int view_id;
    int leader_id;
    bool is_leader;
    std::set<int> membership_list;
    std::set<int> alive_processes;
    std::vector<std::string> host_list;
    std::string my_hostname;
    int tcp_port;
    int udp_port;
    int sequence_num;
    PendingOperation pending_op;
    

    std::map<int, std::chrono::steady_clock::time_point> heartbeat_times;
    std::mutex mtx;
    
    //std::atomic<bool> join_thread_finished(false);  // Flag to indicate when the thread finishes


    int serverSocket;  // Listening socket for incoming connections
    int udp_fd;

    std::map<RequestID, std::set<int>> ok_responses;

    // Separate maps for incoming and outgoing connections
    std::map<int, int> incoming_connections;
    std::map<int, int> outgoing_connections;

    // Variables for the -c and -t options
    bool crash_after_join;  // Flag for -c option
    int crash_delay;
    bool testcase4;         // Flag for -t option

    Peer(int peer_id, const std::vector<std::string>& hosts, const std::string& my_host);

    void start();
    void Crash();

private:
    void setupConnections();
    void acceptConnections();
    void establishOutgoingConnections();
    void listenForMessages();
    int recv_message_tcp(int sockfd, Message& msg);
    int generate_nonce();

    void udp_listener();
    void heartbeat_sender();
    void failure_detector();

    void handleMessage(const Message& msg, int senderId);

    void handle_que(const Message& msg);
    void handle_resp(const Message& msg);

    void handle_join(int peer_id);
    void handle_req(const Message& msg);
    void handle_ok(const Message& msg);
    void handle_newview(const Message& msg);
    void handle_newleader(const Message& msg);
    void handle_complete_pending(const Message& msg);

    void send_message_tcp(int dest_peer_id, const Message& msg);
    void broadcast_message_tcp(const Message& msg, const std::set<int>& dest_peers);

    void send_heartbeat();

    void serialize_message(const Message& msg, std::string& data);
    void deserialize_message(const std::string& data, Message& msg);

    int get_peer_index(int peer_id) {
        return peer_id - 1;
    }

    int select_new_leader();
    void print_member();
    void run_testcase4();  // Function for -t option

    
};

Peer::Peer(int peer_id, const std::vector<std::string>& hosts, const std::string& my_host)
    : id(peer_id), host_list(hosts), my_hostname(my_host), udp_port(6000), sequence_num(0) {
    tcp_port = PORT + id;  // Unique port per peer
    leader_id = 1;
    is_leader = (id == leader_id);
    view_id = 0;
    membership_list.insert(peer_id);
    alive_processes.insert(peer_id);
    testcase4=false;
    

    // Initialize UDP socket
    udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_fd < 0) {
        perror("Error opening UDP socket");
        exit(1);
    }

    struct sockaddr_in udp_addr;
    memset(&udp_addr, 0, sizeof(udp_addr));
    udp_addr.sin_family = AF_INET;
    udp_addr.sin_addr.s_addr = INADDR_ANY;
    udp_addr.sin_port = htons(udp_port);

    if (bind(udp_fd, (struct sockaddr*)&udp_addr, sizeof(udp_addr)) < 0) {
        perror("Error binding UDP socket");
        exit(1);
    }
}

void Peer::start() {
    setupConnections();  // Set up TCP connections

    // Start UDP listener
    std::thread udp_thread(&Peer::udp_listener, this);
    udp_thread.detach();

    // Start heartbeat sender and failure detector
    std::thread hb_thread(&Peer::heartbeat_sender, this);
    hb_thread.detach();
    std::thread fd_thread(&Peer::failure_detector, this);
    fd_thread.detach();
     if (is_leader && testcase4) {
        // Run Testcase 4 logic
        std::cerr<<"testcase4 is true \n";
        std::thread testcase_thread(&Peer::run_testcase4, this);
        testcase_thread.detach();
    }

    if (!is_leader) {
        std::thread join_thread([this]() {
            // Try to discover the leader and join

            bool joined = false;
            bool askFor=false;
            while (!joined) {
                //std::cerr << "{peer_id:" << id << ", message: membership's size: "<< membership_list.size()  <<"\n";

                {
                std::lock_guard<std::mutex> lock(mtx);
                    
                if (membership_list.size() > 1) {
                    //std::cerr<<"I 'm ending this endless world"<<id;
                    joined = true;
                    break;
                }
                }
                //std::cerr <<"[debug]"<< "{peer_id:" << id << ", message:\"Attempting to discover leader...\"}\n";

                // Send QUE messages to other peers
                
                for (int peer_id = 1; peer_id <= (int)host_list.size(); ++peer_id) {
                    if (peer_id != id) {
                        Message que_msg;
                        que_msg.type = QUE;
                        que_msg.peer_id = id;  // Sender
                        send_message_tcp(peer_id, que_msg);
                    }
                }

                // Wait to see if we get a response with the current leader
                std::this_thread::sleep_for(std::chrono::seconds(5));

                if (leader_id != -1) {
                    // Now send the JOIN message to the discovered leader
                    Message join_msg;
                    join_msg.type = JOIN;
                    join_msg.peer_id = id;
                    send_message_tcp(leader_id, join_msg);

                    if (crash_after_join) {
                        std::this_thread::sleep_for(std::chrono::seconds(crash_delay));
                        Crash();
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10000));
                
            }
            //std::terminate();
            //std::cerr<<"The world is terminated";
            //std::cerr << "{peer_id:" << id << ", message:\"Successfully joined the group with leader " << leader_id << "\"}\n";
        });
        join_thread.detach();
    }

    // Start the message handling loop
    listenForMessages();
}

void Peer::setupConnections() {
    // Create a single server socket for incoming connections
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        perror("Failed to create server socket");
        exit(1);
    }

    // Set server address and bind it to the process port (tcp_port)
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = inet_addr("0.0.0.0");
    serverAddr.sin_port = htons(tcp_port);  // Unique port for each peer

    if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Failed to bind server socket");
        exit(1);
    }

    if (listen(serverSocket, 5) < 0) {
        perror("Failed to listen on server socket");
        exit(1);
    }

    //std::cerr <<"[debug]"<< my_hostname << " Server listening on port " << tcp_port << std::endl;

    // Start a thread to accept incoming connections
    std::thread acceptThread(&Peer::acceptConnections, this);
    acceptThread.detach();

    // Establish outgoing connections to other peers
    std::thread connectThread(&Peer::establishOutgoingConnections, this);
    connectThread.detach();
}

void Peer::acceptConnections() {
    while (true) {
        sockaddr_in clientAddr;
        socklen_t clientAddrLen = sizeof(clientAddr);
        int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientAddrLen);
        if (clientSocket < 0) {
            if (errno == EINTR) continue; // Interrupted by signal, retry
            std::cerr << "Failed to accept client connection" << std::endl;
            continue;
        }

        // Receive the client's peer ID (senderId)
        int senderIdNetOrder;
        int bytesReceived = recv(clientSocket, &senderIdNetOrder, sizeof(senderIdNetOrder), MSG_WAITALL);
        if (bytesReceived <= 0) {
            close(clientSocket);
            continue;
        }
        int senderId = ntohl(senderIdNetOrder);

        // Store the incoming connection based on sender's ID
        {
            std::lock_guard<std::mutex> lock(mtx);
            incoming_connections[senderId] = clientSocket;
            alive_processes.insert(senderId);
        }

        //std::cerr <<"[debug]"<< id << ": Established incoming connection from process " << senderId << std::endl;
    }
}

void Peer::establishOutgoingConnections() {
    int numPeers = host_list.size();
    while (true) {
        for (int i = 1; i <= numPeers; ++i) {
            if (i == id) continue;  // Skip self

            bool needToConnect = false;
            int clientSocket = -1;

            {
                std::lock_guard<std::mutex> lock(mtx);
                if (outgoing_connections.count(i) == 0) {
                    needToConnect = true;
                }//  else {
                //     clientSocket = outgoing_connections[i];
                // }
            }

            if (needToConnect) {
                // Establish a new connection
                int sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd == -1) {
                    std::cerr << "Failed to create client socket" << std::endl;
                    continue;
                }

                sockaddr_in serverAddr;
                serverAddr.sin_family = AF_INET;
                serverAddr.sin_port = htons(PORT + i);  // Port of peer i

                std::string dest_host = host_list[i - 1];
                struct hostent* server = gethostbyname(dest_host.c_str());
                if (server == NULL) {
                    // std::cerr << "Failed to resolve host " << dest_host << std::endl;
                    close(sockfd);
                    continue;
                }
                memcpy(&serverAddr.sin_addr.s_addr, server->h_addr, server->h_length);

                // Try to connect
                if (connect(sockfd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
                    // std::cerr << "Failed to connect to peer " << i << std::endl;
                    close(sockfd);
                    continue;
                }

                // Send our peer id to the server
                int net_order_id = htonl(id);
                if (send(sockfd, &net_order_id, sizeof(net_order_id), 0) != sizeof(net_order_id)) {
                    close(sockfd);
                    continue;
                }

                // Store the outgoing connection
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    outgoing_connections[i] = sockfd;
                    alive_processes.insert(i);
                }

                //std::cerr << my_hostname << " [debug]Established outgoing connection to process " << dest_host << " on port " << (PORT + i) << std::endl;

           }   else if (clientSocket != -1) {
                 // Check if existing connection is still valid
                char buffer;
                int result = recv(clientSocket, &buffer, 1, MSG_PEEK | MSG_DONTWAIT);
                 if (result == 0 || (result == -1 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                     // Connection is broken
                     //std::cerr << my_hostname << " Connection to process " << i << " is broken. Reconnecting..." << std::endl;
                     close(clientSocket);
                    {
                         std::lock_guard<std::mutex> lock(mtx);
                         outgoing_connections.erase(i);
                         alive_processes.erase(i);
                     }
                     // Try to re-establish the connection in the next iteration
                 }
             }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));  // Retry every 30 second
    }
}

void Peer::listenForMessages() {
    while (true) {
        fd_set readfds;
        FD_ZERO(&readfds);

        int max_sd = -1;
        std::vector<int> invalid_connections;

        // Add incoming sockets to the read set
        {
            std::lock_guard<std::mutex> lock(mtx);
            for (const auto& conn : incoming_connections) {
                int sockfd = conn.second;
                // Check if the connection is still valid
                char buffer;
                int result = recv(sockfd, &buffer, 1, MSG_PEEK | MSG_DONTWAIT);
                if (result == 0 || (result == -1 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                    // Connection is invalid or broken, mark it for removal
                    invalid_connections.push_back(conn.first);
                    continue;
                }
                // Add valid connections to the file descriptor set
                FD_SET(sockfd, &readfds);
                if (sockfd > max_sd) {
                    max_sd = sockfd;
                }
            }
        }

        // Remove invalid connections outside of the lock to avoid deadlock
        if (!invalid_connections.empty()) {
            std::lock_guard<std::mutex> lock(mtx);
            for (int peer_id : invalid_connections) {
                if (incoming_connections.count(peer_id)) {
                    close(incoming_connections[peer_id]);
                    incoming_connections.erase(peer_id);
                    alive_processes.erase(peer_id);
                    std::cerr << "Connection to peer " << peer_id << " removed due to disconnection." << std::endl;
                }
            }
        }

        if (max_sd == -1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // Set a timeout for select()
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000; // 100ms timeout

        int activity = select(max_sd + 1, &readfds, NULL, NULL, &timeout);

        if (activity < 0) {
            if (errno == EINTR) continue;
            std::cerr << "Select error" << std::endl;
            continue;
        }

        // Handle incoming messages on valid connections
        std::map<int, int> localIncomingConnections;
        {
            std::lock_guard<std::mutex> lock(mtx);
            localIncomingConnections = incoming_connections;
        }
        std::vector<int> disconnected;
        for (const auto& conn : localIncomingConnections) {
            int peer_id = conn.first;
            int sockfd = conn.second;

            if (FD_ISSET(sockfd, &readfds)) {
                // Read the message
                Message msg;
                int result = recv_message_tcp(sockfd, msg);
                if (result <= 0) {
                    // Connection closed or error, mark for removal
                    close(sockfd);
                    disconnected.push_back(peer_id);
                    std::cerr << "Peer " << peer_id << " disconnected" << std::endl;
                } else {
                    handleMessage(msg, peer_id);
                }
            }
        }

        // Remove disconnected sockets
        if (!disconnected.empty()) {
            std::lock_guard<std::mutex> lock(mtx);
            for (int peer_id : disconnected) {
                incoming_connections.erase(peer_id);
                alive_processes.erase(peer_id);
            }
        }
    }
}


int Peer::recv_message_tcp(int sockfd, Message& msg) {
    char length_buffer[4];
    int n = recv(sockfd, length_buffer, 4, MSG_WAITALL);
    if (n <= 0) {
        return n;
    }

    uint32_t data_length;
    memcpy(&data_length, length_buffer, 4);
    data_length = ntohl(data_length);
    if (data_length > 10485760) {
        std::cerr << "Received message with unreasonable size: " << data_length << std::endl;
        return -1;
    }


    std::vector<char> buffer(data_length);
    n = recv(sockfd, buffer.data(), data_length, MSG_WAITALL);
    if (n <= 0) {
        return n;
    }

    std::string data(buffer.data(), data_length);
    deserialize_message(data, msg);

    return n;
}

void Peer::udp_listener() {
    while (true) {
        char buffer[1024];
        struct sockaddr_in sender_addr;
        socklen_t addrlen = sizeof(sender_addr);
        int n = recvfrom(udp_fd, buffer, 1024, 0, (struct sockaddr*)&sender_addr, &addrlen);
        if (n < 0) {
            perror("Error on recvfrom");
            continue;
        }

        std::string sender_ip = inet_ntoa(sender_addr.sin_addr);
        int sender_peer_id = -1;
        for (size_t i = 0; i < host_list.size(); ++i) {
            struct hostent* server = gethostbyname(host_list[i].c_str());
            if (server == NULL) continue;
            char* ip = inet_ntoa(*(struct in_addr*)server->h_addr);
            if (sender_ip == ip) {
                sender_peer_id = i + 1;
                break;
            }
        }

        if (sender_peer_id != -1) {
            std::unique_lock<std::mutex> lock(mtx);
            heartbeat_times[sender_peer_id] = std::chrono::steady_clock::now();
            alive_processes.insert(sender_peer_id);
            // std::cerr << "{peer_id:" << id << ", message:\"UDP message received from peer " << sender_peer_id << "\"}\n";
        }
    }
}

void Peer::heartbeat_sender() {
    while (true) {
        // Lock only when accessing shared membership_list
        std::set<int> current_members;

        {
            std::unique_lock<std::mutex> lock(mtx);
            current_members = membership_list;
        }

        for (int peer_id : current_members) {
            if (peer_id == id) continue;

            std::string dest_host = host_list[get_peer_index(peer_id)];
            struct sockaddr_in dest_addr;
            dest_addr.sin_family = AF_INET;
            dest_addr.sin_port = htons(udp_port);

            struct hostent* server = gethostbyname(dest_host.c_str());

            if (server == NULL) {
                //std::cerr << "Error resolving host " << dest_host << std::endl;
                continue;
            }
            memcpy(&dest_addr.sin_addr.s_addr, server->h_addr, server->h_length);

            const char* hb_msg = "HEARTBEAT";
            sendto(udp_fd, hb_msg, strlen(hb_msg), 0, (struct sockaddr*)&dest_addr, sizeof(dest_addr));
            //std::cerr<<"peer"<<id<<"sent a hearatbeat to:"<<dest_host.c_str()<<"\n";
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Heartbeat every second
    }
}

void Peer::failure_detector() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Failure check every 500 ms
        auto now = std::chrono::steady_clock::now();

        std::set<int> failed_peers;
        {
            // Narrow the scope of the lock to the shared data access
            std::unique_lock<std::mutex> lock(mtx);
            for (int peer_id : membership_list) {
                if (peer_id == id) continue;

                auto hb_it = heartbeat_times.find(peer_id);
                if (hb_it != heartbeat_times.end()) {
                    auto last_hb = hb_it->second;
                    if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_hb).count() > 3000) {
                        std::cerr << "{peer_id:" << id << ", view_id:" << view_id << ", leader:" << leader_id
                                  << ", message:\"peer " << peer_id;
                        if (peer_id == leader_id) {
                            std::cerr << " (leader)";
                        }
                        std::cerr << " unreachable\"}\n";
                        failed_peers.insert(peer_id);
                        heartbeat_times.erase(hb_it);
                        alive_processes.erase(peer_id);
                    }
                }
            }

            for (int failed_peer_id : failed_peers) {
                membership_list.erase(failed_peer_id);
                if (incoming_connections.count(failed_peer_id)) {
                    close(incoming_connections[failed_peer_id]);
                    incoming_connections.erase(failed_peer_id);
                }
                if (outgoing_connections.count(failed_peer_id)) {
                    close(outgoing_connections[failed_peer_id]);
                    outgoing_connections.erase(failed_peer_id);
                }
            }
        }

        // Handle leader selection and sending failure notifications outside of the lock
        if (failed_peers.count(leader_id)) {
            leader_id = select_new_leader();
            is_leader = (id == leader_id);

            if (is_leader) {
                Message newleader_msg;
                newleader_msg.type = NEWLEADER;
                newleader_msg.view_id = ++view_id;
                newleader_msg.leader_id = leader_id;
                {
                    std::unique_lock<std::mutex> lock(mtx);
                    newleader_msg.memb_list.assign(membership_list.begin(), membership_list.end());
                }

                std::cerr << "{peer_id:" << id << ", view_id:" << view_id << ", leader:" << leader_id
                          << ", message:\"I am the new leader\"}\n";

                std::set<int> current_members;
                {
                    std::unique_lock<std::mutex> lock(mtx);
                    current_members = membership_list;
                }

                for (int peer_id : current_members) {
                    if (peer_id != id) {
                        send_message_tcp(peer_id, newleader_msg);
                    }
                }
            }
        }

        if (is_leader) {
            for (int failed_peer_id : failed_peers) {
                RequestID req_id;

                {
                std::unique_lock<std::mutex> lock(mtx);
                req_id = {leader_id, generate_nonce()};
                }

                Message req_msg;
                req_msg.type = REQ;
                req_msg.request_id = req_id;
                req_msg.view_id = view_id;
                req_msg.operation = DEL;
                req_msg.peer_id = id;
                req_msg.target_peer_id = failed_peer_id;

                std::set<int> dest_peers;
                {
                    std::unique_lock<std::mutex> lock(mtx);
                    dest_peers = membership_list;
                }
                dest_peers.erase(id);
                broadcast_message_tcp(req_msg, dest_peers);
            }
        }
    }
}

void Peer::handleMessage(const Message& msg, int senderId) {
    // Handle different message types


    switch (msg.type) {
        case JOIN:
            handle_join(msg.peer_id);
            break;
        case REQ:
            handle_req(msg);
            break;
        case OK:
            handle_ok(msg);
            break;
        case NEWVIEW:
            handle_newview(msg);
            break;
        case NEWLEADER:
            handle_newleader(msg);
            break;
        case QUE:
            handle_que(msg);
            break;
        case RESP:
            handle_resp(msg);
            break;
        default:
            std::cerr << "Unknown message type received from peer " << senderId << "\n";
            break;
    }
}

void Peer::handle_join(int joining_peer_id) {
    std::unique_lock<std::mutex> lock(mtx);
    if (is_leader) {
        //std::cerr <<"[debug]"<< "membership size:"<<membership_list.size() ;
        if (membership_list.size() == 1) {
            membership_list.insert(joining_peer_id);

            Message newview_msg;
            newview_msg.type = NEWVIEW;
            newview_msg.view_id = ++view_id;
            newview_msg.leader_id = leader_id;
            newview_msg.memb_list.assign(membership_list.begin(), membership_list.end());

            print_member();
            send_message_tcp(joining_peer_id, newview_msg);
           


            //std::cerr << "{peer_id:" << id << ", message:\"New peer " << joining_peer_id << " added directly to membership\"}\n";
        } else {
            RequestID req_id = {leader_id, ++sequence_num};
            Message req_msg;
            req_msg.type = REQ;

            req_msg.request_id = {leader_id, generate_nonce()};

            req_msg.view_id = view_id;
            req_msg.operation = ADD;
            req_msg.peer_id = id;
            req_msg.target_peer_id = joining_peer_id;

            ok_responses[req_id].insert(id);

            std::set<int> dest_peers = membership_list;
            dest_peers.erase(id);
            broadcast_message_tcp(req_msg, dest_peers);
    //         std::cerr << "{peer_id:" << id << ", view_id:" << view_id << ", leader:" << leader_id
    //           << ", memb_list:[";
    // for (auto it = membership_list.begin(); it != membership_list.end(); ++it) {
    //     std::cerr << *it;
    //     if (std::next(it) != membership_list.end()) {
    //         std::cerr << ",";
    //     }
    // }
    // std::cerr << "]}\n";
    //std::cerr <<  "boardcasting:";
        }
    }
}

void Peer::handle_que(const Message& msg) {
    std::unique_lock<std::mutex> lock(mtx);
    if (is_leader || membership_list.size() > 2) {
        Message resp_msg;
        resp_msg.type = RESP;
        resp_msg.peer_id = id;
        resp_msg.leader_id = leader_id;
        send_message_tcp(msg.peer_id, resp_msg);
    }
}

void Peer::handle_resp(const Message& msg) {
    std::unique_lock<std::mutex> lock(mtx);
    if (msg.leader_id != -1) {
        leader_id = msg.leader_id;
        //std::cerr << "{peer_id:" << id << ", message:\"Discovered leader " << leader_id << "\"}\n";
    }
}

void Peer::handle_req(const Message& msg) {
    std::unique_lock<std::mutex> lock(mtx);
    //std::cerr << "peer_id:" << id << ", message handling request ";
    //std::cerr << msg.request_id.leader_id<<msg.request_id.nonce;

    pending_op.operation = msg.operation;
    pending_op.peer_id = msg.target_peer_id;

    Message ok_msg;
    ok_msg.type = OK;
    ok_msg.request_id = msg.request_id;
    ok_msg.view_id = view_id;
    ok_msg.peer_id = id;
    ok_msg.operation=msg.operation;
    ok_msg.target_peer_id = msg.target_peer_id;
    send_message_tcp(leader_id, ok_msg);
}

void Peer::handle_ok(const Message& msg) {
    std::unique_lock<std::mutex> lock(mtx);
    //std::cerr << "[debug]: handling ok from :" << msg.peer_id << " to " << bool(msg.operation == ADD) << ":" << msg.target_peer_id<<"\n";
    if (is_leader) {
        ok_responses[msg.request_id].insert(msg.peer_id);

        int expected_oks = membership_list.size() - 1;
        //std::cerr << "[debug]: (ok_responses[msg.request_id].size() == expected_oks) :" << (ok_responses[msg.request_id].size() == expected_oks)<<"\n";
        //std::cerr<<"[debug] \n";
       // std::cerr <<  ok_responses[msg.request_id].size()<<"::"<< expected_oks<<"\n";


        if (ok_responses[msg.request_id].size() == expected_oks) {
            view_id++;
            if (msg.operation == ADD) {
                membership_list.insert(msg.target_peer_id);
            } else if (msg.operation == DEL) {
                membership_list.erase(msg.target_peer_id);
            }

            Message newview_msg;
            newview_msg.type = NEWVIEW;
            newview_msg.view_id = view_id;
            newview_msg.memb_list.assign(membership_list.begin(), membership_list.end());
            newview_msg.leader_id = leader_id;
            //std::cerr<<"[debug] sending new view~";
            print_member();
            for (int peer_id : membership_list) {
                send_message_tcp(peer_id, newview_msg);
            }

            ok_responses.erase(msg.request_id);
        }
    }
}


void Peer::handle_newview(const Message& msg) {
    std::unique_lock<std::mutex> lock(mtx);
    view_id = msg.view_id;
    membership_list.clear();
    membership_list.insert(msg.memb_list.begin(), msg.memb_list.end());
    leader_id = msg.leader_id;
    is_leader = (id == leader_id);

    print_member();
}

void Peer::handle_newleader(const Message& msg) {
     std::unique_lock<std::mutex> lock(mtx);
    

    // If there is a pending operation, notify the new leader
    if (pending_op.exists) {
        Message complete_msg;
        complete_msg.type = COMPLETE_PENDING;
        complete_msg.peer_id = id;
        complete_msg.view_id = view_id;
        complete_msg.operation = pending_op.operation;
        complete_msg.target_peer_id = pending_op.peer_id;
        send_message_tcp(leader_id, complete_msg);
        pending_op.exists = false;
    }else{
    view_id = msg.view_id;
    leader_id = msg.leader_id;
    is_leader = (id == leader_id);
    //membership_list.clear();
    //membership_list.insert(msg.memb_list.begin(), msg.memb_list.end());

    }
    std::cerr << "{peer_id:" << id << ", view_id:" << view_id << ", leader:" << leader_id
              << ", message:\"New leader elected\"}\n";
}

void Peer::handle_complete_pending(const Message& msg) {
    std::unique_lock<std::mutex> lock(mtx);
    if (is_leader) {
        RequestID req_id = {leader_id, generate_nonce()};
        Message req_msg;
        req_msg.type = REQ;
        req_msg.request_id = req_id;
        req_msg.view_id = msg.view_id;
        req_msg.operation = msg.operation;
        req_msg.peer_id = id;
        req_msg.target_peer_id = msg.target_peer_id;

        ok_responses[req_id].insert(id);

        std::set<int> dest_peers = membership_list;
        dest_peers.erase(id);
        broadcast_message_tcp(req_msg, dest_peers);
    }
}

void Peer::send_message_tcp(int dest_peer_id, const Message& msg) {
    int sockfd;
    {
        //std::unique_lock<std::mutex> lock(mtx);
        if (outgoing_connections.count(dest_peer_id) > 0) {
            sockfd = outgoing_connections[dest_peer_id];
        } else {
            // Connection not established
            return;
        }
    }

    std::string data;
    serialize_message(msg, data);
    uint32_t data_length = htonl(data.size());
    if (send(sockfd, &data_length, sizeof(data_length), 0) != sizeof(data_length)) {
        close(sockfd);
        {
            std::unique_lock<std::mutex> lock(mtx);
            outgoing_connections.erase(dest_peer_id);
            alive_processes.erase(dest_peer_id);
        }
        return;
    }
    if (send(sockfd, data.c_str(), data.size(), 0) != (ssize_t)data.size()) {
        close(sockfd);
        {
            std::unique_lock<std::mutex> lock(mtx);
            outgoing_connections.erase(dest_peer_id);
            alive_processes.erase(dest_peer_id);
        }
        return;
    }


}

void Peer::broadcast_message_tcp(const Message& msg, const std::set<int>& dest_peers) {
    for (int peer_id : dest_peers) {
        send_message_tcp(peer_id, msg);
    }
}

void Peer::send_heartbeat() {
    std::set<int> current_members;
    {
        std::unique_lock<std::mutex> lock(mtx);
        current_members = membership_list;
    }

    for (int peer_id : current_members) {
        if (peer_id == id) continue;
        std::string dest_host = host_list[get_peer_index(peer_id)];
        struct sockaddr_in dest_addr;
        dest_addr.sin_family = AF_INET;
        dest_addr.sin_port = htons(udp_port);

        struct hostent* server = gethostbyname(dest_host.c_str());
        if (server == NULL) {
            // std::cerr << "Error resolving host " << dest_host << std::endl;
            continue;
        }
        memcpy(&dest_addr.sin_addr.s_addr, server->h_addr, server->h_length);

        const char* hb_msg = "HEARTBEAT";
        sendto(udp_fd, hb_msg, strlen(hb_msg), 0, (struct sockaddr*)&dest_addr, sizeof(dest_addr));
    }
}

int Peer::select_new_leader() {
    std::unique_lock<std::mutex> lock(mtx);

    if (membership_list.empty()) {
        return -1;
    }

    int new_leader_id = *membership_list.begin();
    return new_leader_id;
}

void Peer::serialize_message(const Message& msg, std::string& data) {
    data = std::to_string(msg.type) + "|" +
           std::to_string(msg.request_id.leader_id) + "," + std::to_string(msg.request_id.nonce) + "|" +
           std::to_string(msg.view_id) + "|" +
           std::to_string(msg.operation) + "|" +
           std::to_string(msg.peer_id) + "|" +
           std::to_string(msg.target_peer_id) + "|";
    data += "[";
    for (size_t i = 0; i < msg.memb_list.size(); ++i) {
        data += std::to_string(msg.memb_list[i]);
        if (i != msg.memb_list.size() - 1) data += ",";
    }
    data += "]|";
    data += std::to_string(msg.leader_id) + "|" + msg.text;
}

void Peer::deserialize_message(const std::string& data, Message& msg) {
    std::istringstream iss(data);
    std::string token;

    std::getline(iss, token, '|');
    msg.type = static_cast<MessageType>(std::stoi(token));

    std::getline(iss, token, '|');
    size_t comma_pos = token.find(',');
    msg.request_id.leader_id = std::stoi(token.substr(0, comma_pos));
    msg.request_id.nonce = std::stoi(token.substr(comma_pos + 1));

    std::getline(iss, token, '|');
    msg.view_id = std::stoi(token);

    std::getline(iss, token, '|');
    msg.operation = static_cast<OperationType>(std::stoi(token));

    std::getline(iss, token, '|');
    msg.peer_id = std::stoi(token);

    std::getline(iss, token, '|');
    msg.target_peer_id = std::stoi(token);

    std::getline(iss, token, '|');
    if (token.size() >= 2 && token[0] == '[' && token[token.size() - 1] == ']') {
        std::string list_str = token.substr(1, token.size() - 2);
        std::istringstream list_iss(list_str);
        std::string item;
        while (std::getline(list_iss, item, ',')) {
            msg.memb_list.push_back(std::stoi(item));
        }
    }

    std::getline(iss, token, '|');
    msg.leader_id = std::stoi(token);

    std::getline(iss, msg.text);
}

void Peer::print_member(){
    std::cerr << "{peer_id:" << id << ", view_id:" << view_id << ", leader:" << leader_id
              << ", memb_list:[";
    for (auto it = membership_list.begin(); it != membership_list.end(); ++it) {
        std::cerr << *it;
        if (std::next(it) != membership_list.end()) {
            std::cerr << ",";
        }
    }
    std::cerr << "]}\n";
}

int Peer::generate_nonce() {
    return static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count() % 100000000);
}

void Peer::Crash(){
    std::cerr << "{peer_id:" << id 
              << ", view_id:" << view_id 
              << ", leader:" << leader_id 
              << ", message:\"crashing\"}\n";

    std::exit(EXIT_FAILURE);
}

void Peer::run_testcase4() {
    // Wait for all peers to join
    bool all_joined = false;
    while (!all_joined) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::unique_lock<std::mutex> lock(mtx);
        if (membership_list.size() == host_list.size()) {
            all_joined = true;
        }
    }

   

    RequestID req_id = {leader_id, generate_nonce()};
    Message req_msg;
    req_msg.type = REQ;
    req_msg.request_id = req_id;
    req_msg.view_id = view_id;
    req_msg.operation = DEL;
    req_msg.peer_id = id;
    req_msg.target_peer_id = membership_list.size()-1;  // Target is next leader for DEL

    {
        std::unique_lock<std::mutex> lock(mtx);
        ok_responses[req_id].insert(id);
    }

    std::set<int> dest_peers;
    {
        std::unique_lock<std::mutex> lock(mtx);
        dest_peers = membership_list;
    }
    dest_peers.erase(id);
    dest_peers.erase(membership_list.size()-1);  // Exclude next leader

    broadcast_message_tcp(req_msg, dest_peers);

    // Crash
    Crash();
}


int main(int argc, char* argv[]) {
    std::string hostfile;
    std::string hostname;
    int delay = 0;
    int crash_delay = 0;
    bool testcase4=false;


    int opt;
    while ((opt = getopt(argc, argv, "h:d:c:t")) != -1) {
        switch (opt) {
            case 'h':
                hostfile = optarg;
                break;
            case 'd':
                delay = std::stoi(optarg);
                break;
            case 'c':
                crash_delay = std::stoi(optarg);
                break;
            case 't':
                testcase4=true;
                break;
            default:
                std::cerr << "Usage: " << argv[0] << " -h <hostfile> [-d <delay>] [-c <crash_delay>]" << std::endl;
                exit(EXIT_FAILURE);
        }
    }

    // Get hostname from Docker environment
    char* hostname_env = getenv("HOSTNAME");
    if (hostname_env == nullptr) {
        std::cerr << "Error: HOSTNAME environment variable not set." << std::endl;
        exit(EXIT_FAILURE);
    }
    hostname = std::string(hostname_env);

    if (hostfile.empty() || hostname.empty()) {
        std::cerr << "Error: Hostfile and hostname are required." << std::endl;
        exit(EXIT_FAILURE);
    }

    std::vector<std::string> host_list;
    int peer_id = -1;
    std::ifstream infile(hostfile);
    std::string line;
    int line_number = 1;

    while (std::getline(infile, line)) {
        host_list.push_back(line);
        if (line == hostname) {
            peer_id = line_number;
        }
        line_number++;
    }
    infile.close();

    if (peer_id == -1) {
        std::cerr << "Error: Hostname not found in the hosts file." << std::endl;
        exit(EXIT_FAILURE);
    }

    if (delay > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(delay));
    }

    Peer peer(peer_id, host_list, hostname);
     // Set crash flags
    if (crash_delay > 0) {
        peer.crash_after_join = true;
        peer.crash_delay = crash_delay;
    }

    // Set testcase4 flag
    if (testcase4 && peer.is_leader) {
        peer.testcase4 = true;
    }

    peer.start();

    

    return 0;
}
