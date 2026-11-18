import socket

# Open port 9999 and wait for Spark to connect
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("localhost", 9999))
server.listen(1)
print("Waiting for Spark to connect on port 9999...")

conn, address = server.accept()
print("Spark connected! Type a line and press Enter. Type exit to stop.")

while True:
    message = input("Enter message: ")
    if message.lower() == "exit":
        break
    conn.sendall((message + "\n").encode("utf-8"))   # send the line to Spark

conn.close()
server.close()
print("Sender stopped.")
