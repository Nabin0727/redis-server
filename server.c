// Redis Server Implementation 
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <pthread.h>
#include <arpa/inet.h>
#include<time.h>
#include<sys/time.h>

#define BUFFER 1024
#define REDIS_PONG "+PONG\r\n"
#define REDIS_OK "+OK\r\n"
#define GET_ERROR "$-1\r\n"
#define TYPE_ERROR "+none\r\n"

// define structure to store key value pair
struct store_key
{
    char *key;
    char *value;
    double expiration_time;
    struct store_key *next;
};
struct store_key *head = NULL;


// Parse Redis command
int parse_redis_command(char *buffer, char *command, char *arugment1, char *argument2, char *argument3, double *argument4)
{
    char *ptr = buffer;
    int i, value_len;

    if(*ptr != '*')
    {
        return -1;
    }

    ptr++;
    int element_length = atoi(ptr);

    // escaping we find \n for next iteration
    while(*ptr != '\r' && *ptr != '\n')ptr++;  // skipping the digit
    if (*ptr == '\r') ptr++;   // skip \r
    if (*ptr == '\n') ptr++;   // skip \n

    argument3[0] = '\0';
    *argument4 = 0.0;
    for(i = 0; i< element_length; i++)
    {
        if(*ptr == '$')
        {
            ptr++;

            int value_len = atoi(ptr);

            // escaping we find \n for next iteration
            while(*ptr != '\r' && *ptr != '\n')ptr++; 
            if (*ptr == '\r') ptr++;   // skip \r
            if (*ptr == '\n') ptr++;   // skip \n

            if(i == 0)
            {
                strncpy(command, ptr, value_len);
                command[value_len] = '\0';                    
            }
            else if (i == 1){
                strncpy(arugment1, ptr, value_len);
                arugment1[value_len] = '\0';
            }
            else if(i == 2){
                strncpy(argument2, ptr, value_len);
                argument2[value_len] = '\0';
            }
            else if( i == 3){
                strncpy(argument3, ptr, value_len);
                argument3[value_len] = '\0';
            }
            else{
                *argument4 = strtod(ptr, NULL);
            }
            ptr += value_len;

            if(*ptr == '\r')ptr++;
            if(*ptr == '\n')ptr++;
        }
    }

    return 0;
}
// Get current time in ms
double current_time_ms(){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec /1000.0;
}

// Set key value
void set_key(char *key, char *value, char *argument, double *expiration_time)
{
    struct store_key *current = head;

    // checking and updating
    while(current != NULL){
        if(strcmp(current -> key, key) == 0){
            // key exist
            free(current->value);
            current -> value = strdup(value);
            if(argument[0] != '\0' && (strcmp(argument, "PX") || strcmp(argument, "px") == 0))
            {
                if(*expiration_time > 0)
                {
                    current -> expiration_time = current_time_ms() + *expiration_time;
                }
            }
            return;
        }
        // traversing the linked list
        current = current -> next;
    }  

    // if key is new or doesnto exits
    struct store_key *new_node = malloc(sizeof(struct store_key));
    new_node -> key = strdup(key);
    new_node -> value = strdup(value);
    
     if(argument[0] != '\0' && (strcmp(argument, "PX") || strcmp(argument, "px") == 0 )){
        if(*expiration_time > 0.0){
            new_node -> expiration_time = current_time_ms() + *expiration_time;
        }
        else{
        new_node -> expiration_time = 0.0;
        }
    }
    new_node -> next = head;
    head = new_node;
}
// delete expired key
void delete_key( struct store_key **head, const char *key)
{
    struct store_key *current = *head;
    struct store_key *prev = NULL;

    while (current != NULL)
    {
        if(strcmp(current->key, key) == 0)
        {
            if(prev == NULL)
            {
                *head = current -> next; // deleting first node
            }
            else{
                prev -> next = current -> next;
            }

            free(current->key);
            free(current->value);
            free(current);
            return;
        }
        prev = current;
        current = current->next;
    }
}

// GET key value
char *get_key_value(char *key)
{
    struct store_key *current = head;

    while(current != NULL){
        if(strcmp(current -> key, key) == 0)
        {
            if(current -> expiration_time == 0.0){
                //test to check if works fine
                //printf("\nprogram is here for expiration 0.0!!!\n");
                return current -> value;
            }
            else{
                if(current -> expiration_time < current_time_ms())
                {
                    //printf("\nprogram is here before delete key call!!!\n");
                    // TODO: more dynamic than hardcode for PX
                    delete_key(&head, current->key);
                    return NULL;
                }
                else{
                    return current -> value;
                }
            }
        }
        current = current -> next;
    }
    return NULL;
}

// Get key value type

char *get_key_value_type(const char *key){
    struct store_key *current = head;

    while(current != NULL){
        if(strcmp(current -> key, key) == 0)
        {
            if(current -> expiration_time == 0.0){
                //test to check if works fine
                //printf("\nprogram is here for expiration 0.0!!!\n");
                return "string";
            }
            else{
                if(current -> expiration_time < current_time_ms())
                {
                    //printf("\nprogram is here before delete key call!!!\n");
                    // TODO: more dynamic than hardcode for PX
                    delete_key(&head, current->key);
                    return NULL;
                }
                else{
                    return "string";
                }
            }
        }
        current = current -> next;
    }
    return NULL;
}

// Handle each client
void* handle_client(void *arg)
{
    int client_socket = *(int*)arg;
    free(arg);

    char buffer[BUFFER];
    char command[BUFFER];
    char argument1[BUFFER];
    char argument2[BUFFER];
    char argument3[BUFFER];
    double argument4;

    int bytes;

    while ((bytes = recv(client_socket, buffer, sizeof(buffer) - 1, 0)) > 0) {
        buffer[bytes] = '\0';
        if (parse_redis_command(buffer, command, argument1, argument2, argument3, &argument4) == 0) {
            if (strcmp(command, "PING") == 0) {
                send(client_socket, REDIS_PONG, strlen(REDIS_PONG), 0);
            } else if (strcmp(command, "ECHO") == 0) {
                char response[BUFFER];
                snprintf(response, sizeof(response), "$%zu\r\n%s\r\n", strlen(argument1), argument1);
                send(client_socket, response, strlen(response), 0);
            }
            else if(strcmp(command, "SET") == 0)
            {
                set_key(argument1, argument2, argument3, &argument4);
                send(client_socket, REDIS_OK, strlen(REDIS_OK), 0);
            }
            else if (strcmp(command, "GET") == 0)
            {
                char *key_value = get_key_value(argument1);

                if (key_value == NULL)
                {
                    send(client_socket, GET_ERROR, strlen(GET_ERROR), 0);
                }else{

                    char response[BUFFER];
                    snprintf(response, sizeof(response), "$%zu\r\n%s\r\n", strlen(key_value), key_value);
                    send(client_socket, response, strlen(response), 0);
                }
            }
            else if(strcmp(command, "TYPE") == 0){
                char *key_type = get_key_value_type(argument1);
                if(key_type == NULL)
                {
                    send(client_socket, TYPE_ERROR, strlen(TYPE_ERROR), 0);
                }
                else{
                    char response[BUFFER];
                    snprintf(response, sizeof(response), "+%s\r\n", key_type);
                    send(client_socket, response, strlen(response), 0);
                }
            }
            else if(strcmp(command, "XADD") == 0){
                send(client_socket, "Hello World", 11, 0);
            }
            else {
                //send(client_socket, "-ERR unknown command\r\n", 23, 0);
                send(client_socket, command, strlen(command), 0);
            }
        }
    }

    printf("Client disconnected\n");
    close(client_socket);
    return NULL;
}

int main()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket failed");
        exit(1);
    }

    // this is necessary for resuing the same port for same IP.
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        perror("SO_REUSEADDR failed");
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_port = htons(6379);
    address.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(1);
    }

    if (listen(server_fd, 10) < 0) {
        perror("Listen failed");
        exit(1);
    }

    printf("Server listening on port 6379...\n");

    int addrlen = sizeof(address);

    while (1) {
        int *client_socket = malloc(sizeof(int));
        *client_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
        if (*client_socket == -1) {
            perror("Accept failed");
            free(client_socket);
            continue;
        }
        
        // handeling multiple process
        pthread_t tid;
        pthread_create(&tid, NULL, handle_client, client_socket);
        pthread_detach(tid);
    }

    close(server_fd);
    return 0;
}

