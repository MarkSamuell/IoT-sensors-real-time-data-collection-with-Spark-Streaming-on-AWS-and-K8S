import socket
import sys

def test_kafka_connection(host, port):
    try:
        # Create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)  # 5 second timeout
        
        # Try to connect
        result = sock.connect_ex((host, port))
        
        if result == 0:
            print(f"Success: Connected to {host}:{port}")
            
            # Try to get the address info that Kafka would use
            addr_info = socket.getaddrinfo(host, port, family=socket.AF_INET)
            print(f"Address info: {addr_info}")
        else:
            print(f"Failed: Could not connect to {host}:{port}")
            print(f"Error code: {result}")
        
        sock.close()
        
    except socket.error as e:
        print(f"Socket error: {e}")
    except Exception as e:
        print(f"General error: {e}")

if __name__ == "__main__":
    host = "k8s-dev-uraeuska-9b50103fb8-97261e472eda8ae5.elb.eu-central-1.amazonaws.com"
    port = 9092
    
    print(f"Testing connection to Kafka broker at {host}:{port}")
    test_kafka_connection(host, port)