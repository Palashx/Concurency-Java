# Concurency-Java (java.util.concurrent)

This was the final project for the course COMP6411 Comparative study of programming language. Main task of the project was to create a dynamic number of threads, and to pass messages between these threads. We were given a text file which had a list of people, and where each person communicates with other people. Linked blocking Queue was used to provide logical concurrency in the program.

Here the main thread reads data from a text file. And it creates other threads. One thread is generated for each person. Depending on the data in text file each thread sends a message to other people, this message contains a time stamp. So there are sender reciever pairs. After receiveing a message from the senderthread, receiver sends a message to the main thread that communication has taken place and main thread prints out on the console with the details of the communication (name of the sender, name of the receiver, timestamp of when the message was sent.
