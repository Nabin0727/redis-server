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

// Definers
#define BUFFER 1024
#define REDIS_PONG "+PONG\r\n"
#define REDIS_OK "+OK\r\n"
#define GET_ERROR "$-1\r\n"
#define TYPE_ERROR "+none\r\n"
#define ZERO_ERROR "-ERR The ID specified in XADD must be greater than 0-0\r\n"
#define ID_SMALLER_ERROR "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

// define structure to store key value pair
struct store_key
{
    char *key;
    char *value;
    double expiration_time;
    struct store_key *next;
};
struct store_key *head = NULL;

// defining structure for stream input

struct stream_entry{
    char *id;
    char *field;
    char *value;
    struct stream_entry *next;
};

struct stream{
    char *name;
    struct stream_entry *entries;
    struct stream *next;
};

struct stream *stream_head = NULL; //we can add typedef infront of the struct for avoid using struct 

// defining struct for stroing the matched data

struct key_value_result{
    char *filed;
    char *value;
};

struct entry_result{
    char *id;
    struct key_value_result *values;
    int num_fileds;
};

// RESP Parser
static int parse_redis_command(char *buffer, char *command, char ***argv, int *argc){
    char *ptr = buffer;

    if(*ptr != '*')
    {
        return -1;
    }
    ptr++;
    int elements = atoi(ptr);
    while (*ptr != '\r' && *ptr != '\n') ptr++;
    if (*ptr == '\r') ptr++;
    if (*ptr == '\n') ptr++;

    if (elements <= 0) return -1;

    *argc = elements - 1;
    *argv = NULL;
    if (*argc > 0) {
        *argv = (char**)malloc(sizeof(char*) * (*argc));
        if (!*argv) return -1;
    }

    int argi = -1; // 0 is command
    for (int i = 0; i < elements; i++) {
        if (*ptr != '$') { // simple implementation supports only bulk strings
            return -1;
        }
        ptr++;
        int len = atoi(ptr);
        while (*ptr != '\r' && *ptr != '\n') ptr++;
        if (*ptr == '\r') ptr++;
        if (*ptr == '\n') ptr++;

        if (len < 0) return -1; // no null bulk strings for command parsing here

        char *value = (char*)malloc(len + 1);
        if (!value) return -1;
        strncpy(value, ptr, len);
        value[len] = '\0';
        ptr += len;

        if (*ptr == '\r') ptr++;
        if (*ptr == '\n') ptr++;

        if (i == 0) {
            // command
            strcpy(command, value);
            free(value);
        } else {
            (*argv)[++argi] = value;
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
            if(argument[0] != '\0' && (strcmp(argument, "PX") == 0 || strcmp(argument, "px") == 0))
            {
                if(*expiration_time > 0)
                {
                    current -> expiration_time = current_time_ms() + *expiration_time;
                }
                return; // retutn after creating
            }
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
    struct stream *current_stream = stream_head;

    while(current_stream != NULL)
    {
        if(strcmp(current_stream -> name, key) == 0)
        {
            return "stream";
        }
        current_stream = current_stream -> next;
    }

    return NULL;
}

// Helper: create a new stream
struct stream *create_stream(const char *stream_name) {
    struct stream *new_stream = malloc(sizeof(struct stream));
    new_stream->name = strdup(stream_name);
    new_stream->entries = NULL;
    new_stream->next = stream_head;
    stream_head = new_stream;
    return new_stream;
}

// Parse ID string into ms and seq
void parse_id(const char *id, int *ms, int *seq, int *wildcard, int *single_id) {
    if(strcmp(id, "*") == 0){
        *single_id = 1;

    }else{
        char *copy = strdup(id);
        *ms = atoi(strtok(copy, "-"));
        char *seq_t = strtok(NULL, "-");

        if(strcmp(seq_t, "*") == 0){
            *wildcard = 1;
            *seq = 0;
        }
        else{
            *seq = atoi(seq_t);
        }
        free(copy);
    }
    //free(seq_t);
}

// Add stream entry with monotonic ID checks
char *add_stream(char *stream_name, const char *id, const char *field, const char *value) {
    struct stream *current = stream_head;
    char *final_id = NULL;
    long long msd;

    // Search for stream
    while (current != NULL) {
        if (strcmp(current->name, stream_name) == 0) {
            break;
        }
        current = current->next;
    }

    // Create stream if not exists
    if (current == NULL) {
        current = create_stream(stream_name);
    }    

    // Parse new ID
    int ms, seq, wildcard = 0, single_id = 0;
    parse_id(id, &ms, &seq, &wildcard, &single_id);

    // Rule 1: Reject 0-0
    if (!wildcard && ms == 0 && seq == 0) {
        return ZERO_ERROR;
    }

    if(single_id)
    {
       msd = (long long)current_time_ms();
        seq = 0;

    }else{
        // checking for increment
        if(wildcard){
            if(current -> entries == NULL)
            {
                seq = seq + 1;
            }else{
                struct stream_entry *last = current->entries;
                while (last->next)
                {
                    last = last -> next;
                }
                int last_ms, last_seq, last_wildcard, single_id;
                parse_id(last->id, &last_ms, &last_seq, &last_wildcard, &single_id);
                
                if(ms == last_ms){
                    seq = last_seq +1;
                }
                else{
                    seq = 0;
                }
            }
        }
        else{
                // Rule 2: Ensure ID is strictly increasing
                if (current->entries != NULL) {
                struct stream_entry *last = current->entries;
                while (last->next != NULL) {
                    last = last->next;
                }
                int last_ms, last_seq, last_wildcard, single_id;
                parse_id(last->id, &last_ms, &last_seq, &last_wildcard, &single_id);

                if (ms < last_ms || (ms == last_ms && seq <= last_seq)) {
                    return ID_SMALLER_ERROR;
                }
            }
        }
    }

    // Calculating final ID
    char new_id[50];
    if(single_id){
        snprintf(new_id, sizeof(new_id), "%lld-%d", msd, seq);

    }else{
        snprintf(new_id, sizeof(new_id), "%d-%d", ms, seq);
    }
    // snprintf(new_id, sizeof(new_id), "%d-%d", ms, seq);
    // final_id = malloc(strlen(new_id) +1);
    // strcpy(final_id, new_id);

    // Create new entry
    struct stream_entry *new_entry = malloc(sizeof(struct stream_entry));

    new_entry -> id = strdup(new_id);
    new_entry->field = strdup(field);
    new_entry->value = strdup(value);
    new_entry->next = NULL;

    // Insert at end of list
    if (current->entries == NULL) {
        current->entries = new_entry;
    } else {
        struct stream_entry *entry = current->entries;
        while (entry->next != NULL) {
            entry = entry->next;
        }
        entry->next = new_entry;
    }

    return new_entry->id;  // success, return the ID
}

// Returns -1 if id1 < id2, 0 if equal, 1 if id1 > id2
int compare_ids(const char *id1, const char *id2) {
    long long ms1 = 0, ms2 = 0;
    int seq1 = 0, seq2 = 0;

    // Parse milliseconds and sequence
    sscanf(id1, "%lld-%d", &ms1, &seq1);
    sscanf(id2, "%lld-%d", &ms2, &seq2);

    if (ms1 < ms2) return -1;
    if (ms1 > ms2) return 1;

    // If milliseconds are equal, compare sequence
    if (seq1 < seq2) return -1;
    if (seq1 > seq2) return 1;

    return 0; // Both are equal
}

// Get stream for range command
char *get_stream(const char *stream_name, const char *id_start, const char *id_end) {
    struct stream *current = stream_head;
    struct stream_entry *entry;
    char *resp;
    size_t buffer_size = 2048;  // initial size, bigger since nesting
    resp = malloc(buffer_size);
    if (!resp) return NULL;
    resp[0] = '\0';
    size_t len = 0;
    int entry_count = 0;

    // Find the stream
    while (current) {
        if (strcmp(current->name, stream_name) == 0) break;
        current = current->next;
    }
    if (!current) {
        snprintf(resp, buffer_size, "*0\r\n");  // empty array if stream not found
        return resp;
    }

    // Iterate entries
    entry = current->entries;
    while (entry) {
        if (compare_ids(entry->id, id_start) >= 0 && compare_ids(entry->id, id_end) <= 0) {
            
            // Build field-value array
            char fv_buffer[512];
            int fv_len = snprintf(fv_buffer, sizeof(fv_buffer),
                "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                strlen(entry->field), entry->field,
                strlen(entry->value), entry->value);

            // Now wrap id + field-value-array in another array of 2
            char entry_buffer[1024];
            int written = snprintf(entry_buffer, sizeof(entry_buffer),
                "*2\r\n$%zu\r\n%s\r\n%s",
                strlen(entry->id), entry->id,
                fv_buffer);

            // Resize main buffer if needed
            if (len + written + 32 > buffer_size) {
                buffer_size *= 2;
                resp = realloc(resp, buffer_size);
            }

            // Append entry to resp
            memcpy(resp + len, entry_buffer, written);
            len += written;
            resp[len] = '\0';
            entry_count++;
        }
        entry = entry->next;
    }

    // Prepend outer array count
    char header[32];
    int header_len = snprintf(header, sizeof(header), "*%d\r\n", entry_count);

    memmove(resp + header_len, resp, len + 1);  // shift for header
    memcpy(resp, header, header_len);

    return resp;
}

// Handle each client
void* handle_client(void *arg)
{
    int client_socket = *(int*)arg;
    free(arg);

    char buffer[BUFFER];
    char command[BUFFER];
    int bytes;

    while ((bytes = recv(client_socket, buffer, sizeof(buffer) - 1, 0)) > 0) {
        buffer[bytes] = '\0';
        char **argv = NULL;
        int argc = 0;

        if (parse_redis_command(buffer, command, &argv, &argc) != 0) {
            send(client_socket, "-ERR invalid command\r\n", 22, 0);
            continue;
        }

        // Map arguments safely
        char *argument1 = (argc > 0) ? argv[0] : NULL;
        char *argument2 = (argc > 1) ? argv[1] : NULL;
        char *argument3 = (argc > 2) ? argv[2] : NULL;
        char *argument4 = (argc > 3) ? argv[3] : "0.0"; // default

        if (strcmp(command, "PING") == 0) {
            send(client_socket, REDIS_PONG, strlen(REDIS_PONG), 0);
        } 
        else if (strcmp(command, "ECHO") == 0) {
            char *msg = argument1 ? argument1 : "";
            char response[BUFFER];
            snprintf(response, sizeof(response), "$%zu\r\n%s\r\n", strlen(msg), msg);
            send(client_socket, response, strlen(response), 0);
        } 
        else if (strcmp(command, "SET") == 0) {
            if (!argument1 || !argument2) {
                send(client_socket, "-ERR wrong number of arguments for 'SET'\r\n", 44, 0);
            } else {
                double ttl = (argc > 3) ? strtod(argv[3], NULL) : 0.0;
                char *px_flag = (argc > 2) ? argv[2] : "";
                set_key(argument1, argument2, px_flag, &ttl);
                send(client_socket, REDIS_OK, strlen(REDIS_OK), 0);
            }
        } 
        else if (strcmp(command, "GET") == 0) {
            if (!argument1) {
                send(client_socket, "-ERR wrong number of arguments for 'GET'\r\n", 44, 0);
            } else {
                char *key_value = get_key_value(argument1);
                if (!key_value) {
                    send(client_socket, GET_ERROR, strlen(GET_ERROR), 0);
                } else {
                    char response[BUFFER];
                    snprintf(response, sizeof(response), "$%zu\r\n%s\r\n", strlen(key_value), key_value);
                    send(client_socket, response, strlen(response), 0);
                }
            }
        } 
        else if (strcmp(command, "TYPE") == 0) {
            if (!argument1) {
                send(client_socket, "-ERR wrong number of arguments for 'TYPE'\r\n", 45, 0);
            } else {
                char *key_type = get_key_value_type(argument1);
                if (!key_type) {
                    send(client_socket, TYPE_ERROR, strlen(TYPE_ERROR), 0);
                } else {
                    char response[BUFFER];
                    snprintf(response, sizeof(response), "+%s\r\n", key_type);
                    send(client_socket, response, strlen(response), 0);
                }
            }
        } 
        else if (strcmp(command, "XADD") == 0) {
            if (!argument1 || !argument2 || !argument3 || !argument4) {
                send(client_socket, "-ERR wrong number of arguments for 'XADD'\r\n", 46, 0);
            } else {
                char response[BUFFER];
                char *stream_response = add_stream(argument1, argument2, argument3, argument4);
                if(strlen(stream_response) < 20){
                    snprintf(response, sizeof(response), "$%zu\r\n%s\r\n", strlen(stream_response), stream_response);
                    send(client_socket, response, strlen(response), 0);
                }else{
                    //snprintf(response, sizeof(response), "%s\r\n",  stream_response);
                    send(client_socket, stream_response, strlen(stream_response), 0);
                }
            }
        }
        else if(strcmp(command, "XRANGE") == 0){
            if(!argument1 || !argument2 || !argument3){
                send(client_socket, "-ERR wrong number of arguments for 'XADD'\r\n", 46, 0);
            }
            else{
                char response[BUFFER];
                char *xrange_response = get_stream(argument1, argument2, argument3);
                send(client_socket, xrange_response, strlen(xrange_response), 0);
            }
        }
        else {
            send(client_socket, "-ERR unknown command\r\n", 23, 0);
        }

        // Free argv memory to prevent leaks
        for (int i = 0; i < argc; i++) free(argv[i]);
        free(argv);
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

