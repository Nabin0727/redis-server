// Building redis server in C
//
#include<stdio.h>
#include<unistd.h>
#include<sys/socket.h>
#include<stdlib.h>
#include<netinet/in.h>

int main()
{
	int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
	if(socket_fd < 0)
	{
		perror("Failed to create socket...");
		exit(1);
	}

	// socket strcuture address defination
	struct sockaddr_in address;
	address.sin_family = AF_INET;
	address.sin_port = htons(6379);
	address.sin_addr.s_addr = INADDR_ANY;

	if(bind(socket_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
	{
		perror("Failed to bind port 6379..\n");
		exit(1);
	}

	if(listen(socket_fd, 10) < 0)
	{
		perror("Failed to listen to port 6379...\n");
		exit(1);
	}


	// size
	int addressLength = sizeof(address);

	while(1)
	{
		printf("Listening on port 6379.....\n");
		int accept_socket = accept(socket_fd, (struct sockaddr *)&address, (socklen_t *)&addressLength);

	}


	return 0;
}
