/*******************************************************************************
* Multi-Threaded FIFO Server Implementation w/ Queue Limit
*
* Description:
*     A server implementation designed to process client requests in First In,
*     First Out (FIFO) order. The server binds to the specified port number
*     provided as a parameter upon launch. It launches worker threads to
*     process incoming requests and allows to specify a maximum queue size.
*
* Usage:
*     <build directory>/server -q <queue_size> -w <workers> <port_number>
*
* Parameters:
*     port_number - The port number to bind the server to.
*     queue_size  - The maximum number of queued requests
*     workers     - The number of parallel threads to process requests.
*
* Author:
*     Renato Mancuso
*
* Affiliation:
*     Boston University
*
* Creation Date:
*     September 25, 2023
*
* Last Update:
*     September 30, 2024
*
* Notes:
*     Ensure to have proper permissions and available port before running the
*     server. The server relies on a FIFO mechanism to handle requests, thus
*     guaranteeing the order of processing. If the queue is full at the time a
*     new request is received, the request is rejected with a negative ack.
* Kelley Notes: Borrowing code from the HW 3 BUILD SOLUTIONS 
*******************************************************************************/


#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sched.h>
#include <signal.h>
#include <pthread.h>

/* Needed for wait(...) */
#include <sys/types.h>
#include <sys/wait.h>

/* Needed for semaphores */
#include <semaphore.h>

/* Include struct definitions and other libraries that need to be
 * included by both client and server */
#include "common.h"

#define BACKLOG_COUNT 100
#define USAGE_STRING				\
	"Missing parameter. Exiting.\n"		\
	"Usage: %s -q <queue size> -w <number of threads> <port_number>\n"

/* Mutex needed to protect the threaded printf. DO NOT TOUCH */
sem_t * printf_mutex;
/* Synchronized printf for multi-threaded operation */
/* USE sync_printf(..) INSTEAD OF printf(..) FOR WORKER AND PARENT THREADS */
#define sync_printf(...)			\
	do {					\
		sem_wait(printf_mutex);		\
		printf(__VA_ARGS__);		\
		sem_post(printf_mutex);		\
	} while (0)

/* START - Variables needed to protect the shared queue. DO NOT TOUCH */
sem_t * queue_mutex;
sem_t * queue_notify;
/* END - Variables needed to protect the shared queue. DO NOT TOUCH */

struct request_meta {
	struct request request;
	struct timespec receipt_timestamp; 
	struct timespec start_timestamp; 
	struct timespec completion_timestamp; 
	/* ADD REQUIRED FIELDS */
};


struct queue {
	/* ADD REQUIRED FIELDS */
	struct request_meta *req_items;
    int head; 
    int tail; 
    size_t queue_size; 
	size_t max_queue_size;
};


struct connection_params {
	/* ADD REQUIRED FIELDS */
	int queue_size; 
	int thread_count; 
};


struct worker_params {
	/* ADD REQUIRED FIELDS */
	struct queue * the_queue; 
	int conn_socket; 
	int worker_done; 
	int thread_id; 
	pthread_t thread; 
};


/* Helper function to perform queue initialization */
void queue_init(struct queue * the_queue, size_t queue_size)
{
	/* IMPLEMENT ME !! */
	the_queue->head = 0; 
	the_queue->tail = 0; 
	the_queue->queue_size = 0; 
	the_queue->max_queue_size = queue_size; 
	the_queue->req_items = (struct request_meta *)malloc(sizeof(struct request_meta) * queue_size);
}



/* Add a new request <request> to the shared queue <the_queue> */
int add_to_queue(struct request_meta to_add, struct queue * the_queue)
{
	int retval = 0;
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */


	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */


	/* Make sure that the queue is not full */
	if (the_queue->queue_size >= the_queue->max_queue_size/* Condition to check for full queue */) {
		/* What to do in case of a full queue */
		retval = -1; 

		/* DO NOT RETURN DIRECTLY HERE. The
		 * sem_post(queue_mutex) below MUST happen. */
	} else {
		/* If all good, add the item in the queue */
		/* IMPLEMENT ME !!*/
		the_queue->req_items[the_queue->tail] = to_add; 
		the_queue->tail = (the_queue->tail + 1) % the_queue->max_queue_size;
		the_queue->queue_size++; 


		/* QUEUE SIGNALING FOR CONSUMER --- DO NOT TOUCH */
		sem_post(queue_notify);
	}


	/* QUEUE PROTECTION OUTRO START --- DO NOT TOUCH */
	sem_post(queue_mutex);
	/* QUEUE PROTECTION OUTRO END --- DO NOT TOUCH */
	return retval;
}


/* Add a new request <request> to the shared queue <the_queue> */
struct request_meta get_from_queue(struct queue * the_queue)
{
	struct request_meta retval;
	memset(&retval, 0, sizeof(retval));
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_notify);
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */


	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */
	//if (the_queue->queue_size > 0) { //
		retval = the_queue->req_items[the_queue->head];
		the_queue->head = (the_queue->head + 1) % the_queue->max_queue_size; 
		the_queue->queue_size--; 
	//}

	/* QUEUE PROTECTION OUTRO START --- DO NOT TOUCH */
	sem_post(queue_mutex);
	/* QUEUE PROTECTION OUTRO END --- DO NOT TOUCH */
	return retval;
}


void dump_queue_status(struct queue * the_queue)
{
	size_t i = 0;
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */


	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */
	printf("Q:[");
	if (the_queue->queue_size > 0) {
		while(i < the_queue->queue_size) {
			printf("R%lu", the_queue->req_items[(the_queue->head + i) % the_queue->max_queue_size].request.req_id);

			if (i < the_queue->queue_size - 1) {
				printf(",");
			}
			i++; 
		}
	}
	printf("]\n");

	/* QUEUE PROTECTION OUTRO START --- DO NOT TOUCH */
	sem_post(queue_mutex);
	/* QUEUE PROTECTION OUTRO END --- DO NOT TOUCH */
}


/* Main logic of the worker thread */
void* worker_main (void * arg)
{
	struct timespec now;
	struct worker_params * params = (struct worker_params *)arg;
	int conn_socket = params->conn_socket; 
	struct request_meta new_request; 
	struct timespec sent_timestamp; 
	struct response res; 

	/* Print the first alive message. */
	clock_gettime(CLOCK_MONOTONIC, &now);
	sync_printf("[#WORKER#] %lf Worker Thread Alive!\n", TSPEC_TO_DOUBLE(now));


	/* Okay, now execute the main logic. */
	while (params->worker_done == 0) {
		/* IMPLEMENT ME !! Main worker logic. */
		new_request = get_from_queue(params->the_queue);
		if (params->worker_done == 1) {
			break; 
		}
		sent_timestamp = new_request.request.req_timestamp; 
		clock_gettime(CLOCK_MONOTONIC, &new_request.start_timestamp);
		
		get_elapsed_busywait(new_request.request.req_length.tv_sec, new_request.request.req_length.tv_nsec);
        
		clock_gettime(CLOCK_MONOTONIC, &new_request.completion_timestamp);
		res.req_id = new_request.request.req_id; 
		res.reserved = 0; 
		res.ack = 0; //accepted

		send(conn_socket, &res, sizeof(struct response), 0);


		double sent = sent_timestamp.tv_sec + (double)sent_timestamp.tv_nsec / NANO_IN_SEC; 
		double request_length = new_request.request.req_length.tv_sec + (double)new_request.request.req_length.tv_nsec / NANO_IN_SEC; 
		double receipt_timestamp_dc = new_request.receipt_timestamp.tv_sec + (double)new_request.receipt_timestamp.tv_nsec / NANO_IN_SEC; 
		double start_timestamp_dc = new_request.start_timestamp.tv_sec + (double)new_request.start_timestamp.tv_nsec / NANO_IN_SEC; 
		double completion_timestamp_dc = new_request.completion_timestamp.tv_sec + (double)new_request.completion_timestamp.tv_nsec / NANO_IN_SEC; 

		sync_printf("T%d R%lu:%.9lf,%.9lf,%.9lf,%.9lf,%.9lf\n", params->thread_id, new_request.request.req_id, sent, request_length, receipt_timestamp_dc, start_timestamp_dc, completion_timestamp_dc);

		dump_queue_status(params->the_queue);
	}


	return NULL;
}


/* This function will control all the workers (start or stop them). 
 * Feel free to change the arguments of the function as you wish. */
int control_workers(int start_stop_cmd, size_t worker_count, struct worker_params * common_params)
{
	/* IMPLEMENT ME !! */
	
	if (start_stop_cmd == 0) { // Starting all the workers
		/* IMPLEMENT ME !! */
		for (size_t i = 0; i < worker_count; i++) {
			if(pthread_create(&common_params[i].thread, NULL, worker_main, &common_params[i]) != 0) {
				perror("Failed");
				return EXIT_FAILURE; 
			}
		}
	} else { // Stopping all the workers
		/* IMPLEMENT ME !! */
		for (size_t j = 0; j < worker_count; j++) {
			sync_printf("INFO: Asserting termination flag for worker thread...\n");
			common_params[j].worker_done = 1; 
		}
		for (size_t m = 0; m < worker_count; m++) {
			sem_post(queue_notify);
		}
		for (size_t k = 0; k < worker_count; k++) {
			if (pthread_join(common_params[k].thread, NULL) != 0) {
				perror("Failed");
				return EXIT_FAILURE; 
			}
			sync_printf("INFO: Worker thread exited.\n");
		} 
	}
	
	return EXIT_SUCCESS;

	/* IMPLEMENT ME !! */
}


/* Main function to handle connection with the client. This function
 * takes in input conn_socket and returns only when the connection
 * with the client is interrupted. */
void handle_connection(int conn_socket, struct connection_params conn_params)
{
	struct request_meta * req = (struct request_meta *)malloc(sizeof(struct request_meta));
	if (req == NULL) {
		perror("Failed");
		goto cleanup; 
	}
	struct queue * the_queue = (struct queue *)malloc(sizeof(struct queue));
	if (the_queue == NULL) {
		perror("Failed");
		goto cleanup; 
	}
	queue_init(the_queue, conn_params.queue_size);
	struct worker_params * work_parm = (struct worker_params *)malloc(sizeof(struct worker_params) * conn_params.thread_count); 
	if (work_parm == NULL) {
		perror("failed");
		goto cleanup;  
	}

	
	/* IMPLEMENT ME!! Start and initialize all the
	 * worker threads ! */
	for (int i = 0; i < conn_params.thread_count; i++) {
		work_parm[i].the_queue = the_queue; 
		work_parm[i].conn_socket = conn_socket; 
		work_parm[i].worker_done = 0; 
		work_parm[i].thread_id = i; 
	}
	
	int control = control_workers(0, conn_params.thread_count, work_parm);
	if (control == EXIT_FAILURE) {
		perror("failed");
		goto cleanup;
	}
		
	/* We are ready to proceed with the rest of the request
	 * handling logic. */
	size_t in_bytes;
	do {
		/* IMPLEMENT ME: Receive next request from socket. */
		in_bytes = recv(conn_socket, &req->request, sizeof(struct request), 0);
		if(in_bytes <= 0) { 
			break; 
		}
		clock_gettime(CLOCK_MONOTONIC, &(req->receipt_timestamp));
		/* Don't just return if in_bytes is 0 or -1. Instead
		 * skip the response and break out of the loop in an
		 * orderly fashion so that we can de-allocate the req
		 * and resp varaibles, and shutdown the socket. */
		 if (in_bytes > 0) {
			

			int add = add_to_queue(*req, the_queue);
			//printf("add_to_queue value, %d\n", add);
			if (add < 0) { //rejected
				struct response rejected = {.req_id = req->request.req_id, .reserved = 0, .ack = 1};
				send(conn_socket, &rejected, sizeof(struct response), 0);
				clock_gettime(CLOCK_MONOTONIC, &(req->receipt_timestamp));
				double sent = req->request.req_timestamp.tv_sec + (double)req->request.req_timestamp.tv_nsec / NANO_IN_SEC; 
				double length = req->request.req_length.tv_sec + (double)req->request.req_length.tv_nsec / NANO_IN_SEC; 
				double rejected_time = req->receipt_timestamp.tv_sec + (double)req->receipt_timestamp.tv_nsec / NANO_IN_SEC; 
				printf("X%lu:%.9lf,%.9lf,%.9lf\n", req->request.req_id, sent, length, rejected_time);
				dump_queue_status(work_parm->the_queue);
				
			}
		 }


		/* IMPLEMENT ME: Attempt to enqueue or reject request! */
	} while (in_bytes > 0);


	/* IMPLEMENT ME!! Gracefully terminate all the worker threads ! */
	control_workers(1, conn_params.thread_count, work_parm);
	goto cleanup; 

	cleanup:
		if (req != NULL) {
			free(req);
		}
		if (the_queue != NULL) {
			if (the_queue->req_items != NULL) {
				free(the_queue->req_items);
			}
			free(the_queue);
		}
		if (work_parm != NULL) {
			free(work_parm);
		}
	shutdown(conn_socket, SHUT_RDWR);
	close(conn_socket);
	sync_printf("INFO: Client disconnected.\n");
}




/* Template implementation of the main function for the FIFO
 * server. The server must accept in input a command line parameter
 * with the <port number> to bind the server to. */
int main (int argc, char ** argv) {
	int sockfd, retval, accepted, optval, opt;
	in_port_t socket_port;
	struct sockaddr_in addr, client;
	struct in_addr any_address;
	socklen_t client_len;
	 


	struct connection_params conn_params;
	conn_params.queue_size = 0; 
	conn_params.thread_count = 0; 


	/* Parse all the command line arguments */
	/* IMPLEMENT ME!! */
	/* PARSE THE COMMANDS LINE: */
	while((opt = getopt(argc, argv, "q:w:")) != -1) {
		switch(opt) {
			case 'q':
				/* 1. Detect the -q parameter and set aside the queue size in conn_params */
		    	conn_params.queue_size = atoi(optarg);
				printf("INFO: setting queue size as %d\n", conn_params.queue_size);
				if (conn_params.queue_size <= 0) {
					return EXIT_FAILURE; 
				}
				break; 
			case 'w':
				/* 2. Detect the -w parameter and set aside the number of threads to launch */
				conn_params.thread_count = atoi(optarg);
				printf("INFO: setting number of threads as %d\n", conn_params.thread_count);
				if (conn_params.thread_count <= 0) {
					return EXIT_FAILURE; 
				}
				break; 
			default: 
				fprintf(stderr, USAGE_STRING, argv[0]);
				return EXIT_FAILURE; 
		}
	}

/* 3. Detect the port number to bind the server socket to (see HW1 and HW2) */
	if (optind < argc) {
		socket_port = strtol(argv[optind], NULL, 10);
		printf("INFO: setting server port as: %d\n", socket_port);
	} else {
		ERROR_INFO();
		fprintf(stderr, USAGE_STRING, argv[0]);
		return EXIT_FAILURE;
	}

	/* Now onward to create the right type of socket */
	sockfd = socket(AF_INET, SOCK_STREAM, 0);


	if (sockfd < 0) {
		ERROR_INFO();
		perror("Unable to create socket");
		return EXIT_FAILURE;
	}


	/* Before moving forward, set socket to reuse address */
	optval = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&optval, sizeof(optval));


	/* Convert INADDR_ANY into network byte order */
	any_address.s_addr = htonl(INADDR_ANY);


	/* Time to bind the socket to the right port  */
	addr.sin_family = AF_INET;
	addr.sin_port = htons(socket_port);
	addr.sin_addr = any_address;


	/* Attempt to bind the socket with the given parameters */
	retval = bind(sockfd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));


	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to bind socket");
		return EXIT_FAILURE;
	}


	/* Let us now proceed to set the server to listen on the selected port */
	retval = listen(sockfd, BACKLOG_COUNT);


	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to listen on socket");
		return EXIT_FAILURE;
	}


	/* Ready to accept connections! */
	printf("INFO: Waiting for incoming connection...\n");
	client_len = sizeof(struct sockaddr_in);
	accepted = accept(sockfd, (struct sockaddr *)&client, &client_len);


	if (accepted == -1) {
		ERROR_INFO();
		perror("Unable to accept connections");
		return EXIT_FAILURE;
	}


	/* Initilize threaded printf mutex */
	printf_mutex = (sem_t *)malloc(sizeof(sem_t));
	retval = sem_init(printf_mutex, 0, 1);
	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to initialize printf mutex");
		return EXIT_FAILURE;
	}


	/* Initialize queue protection variables. DO NOT TOUCH. */
	queue_mutex = (sem_t *)malloc(sizeof(sem_t));
	queue_notify = (sem_t *)malloc(sizeof(sem_t));
	retval = sem_init(queue_mutex, 0, 1);
	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to initialize queue mutex");
		return EXIT_FAILURE;
	}
	retval = sem_init(queue_notify, 0, 0);
	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to initialize queue notify");
		return EXIT_FAILURE;
	}
	/* DONE - Initialize queue protection variables */


	/* Ready to handle the new connection with the client. */
	handle_connection(accepted, conn_params);

	free(queue_mutex);
	free(queue_notify);
	free(printf_mutex);


	close(sockfd);
	return EXIT_SUCCESS;
}
