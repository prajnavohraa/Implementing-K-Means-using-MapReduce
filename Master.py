import grpc
import mapper_pb2_grpc
# import reducer_pb2_grpc
import mapper_pb2
# import reducer_pb2
from concurrent import futures
import threading
import math
import random
import os
import logging
import time

if(os.path.exists('Data\dump.txt')):
    os.remove('Data\dump.txt')

logging.basicConfig(filename='Data\dump.txt', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

class Master:
    def __init__(self, mappers, reducers, centroids, iterations, mapper_addresses, reducer_addresses):
        self.mappers = mappers
        self.reducers = reducers
        self.centroids = centroids  # Current centroids
        self.previous_centroids = centroids[:]  # Copy of initial centroids
        self.iterations = iterations
        self.mapper_addresses = mapper_addresses
        self.reducer_addresses = reducer_addresses
        self.end_indices=[]
        # for fault tolerance
        self.mapper_status = [True] * mappers
        # self.mapper_status = [True, False]
        # self.mapper_status = [True, True]
        for i in range(mappers):
            self.mapper_status[i]=random.choice([True, False])
        # print(f"mapper_status: {self.mapper_status}")
        self.reducer_status = [True] * reducers
        for i in range(reducers):
            self.reducer_status[i]=random.choice([True, False])
        # self.reducer_status = [True, False, True]
        self.mapper_acks=0
        self.tmp=0
        self.tmp2=0
        self.reducer_acks=0    
        logging.info("Master initialized with centroids: %s", centroids)

    def load_data(self):
        file_path = 'Data\Input\points2.txt'
        with open(file_path, 'r') as file:
            for i, l in enumerate(file):
                pass
        logging.info("Data loaded with %d points", i+1)
        return i + 1
    
    def split_indices(self, data_length):
        chunk_size = data_length // self.mappers
        remainder = data_length % self.mappers
        indices = []
        start_index = 0
        for i in range(self.mappers):
            end_index = start_index + chunk_size + (1 if i < remainder else 0)
            indices.append((start_index, end_index))
            self.end_indices.append(end_index)
            start_index = end_index
        return indices

    #thread loop for send indices to each mapper
    def distribute_data(self):
        logging.info("Distributing data to mappers...")
        data_length = self.load_data()
        index_ranges = self.split_indices(data_length)
        threads = []
        for i, (start_index, end_index) in enumerate(index_ranges):
            thread = threading.Thread(target=self.call_mapper, args=(i, start_index, end_index))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

    #send indices to each mapper
    def call_mapper(self, mapper_index, start_index, end_index):
        try:
            channel = grpc.insecure_channel(self.mapper_addresses[mapper_index])
            stub = mapper_pb2_grpc.MapperStub(channel)
            centroids = [mapper_pb2.Point(x=c[0], y=c[1]) for c in self.centroids]
            request = mapper_pb2.MapRequest(start_index=start_index, end_index=end_index, centroids=centroids, num_mappers=self.mappers, num_reducers=self.reducers)
            
            # Fault Tolerance: Scenario 1
            if not self.mapper_status[mapper_index]:
                logging.info("Mapper %d responded: SIMULATED FAILURE", mapper_index)
                self.tmp+=1
                print(f"Mapper {mapper_index+1} failed with simulated error. Retrying.....")
                print("Sending ERRORSS")
                raise ValueError("Mapper is down. Trying next mapper123...")
            else:
                response = stub.ProcessData(request, timeout=10)
                self.mapper_acks+= 1
                print(f"Mapper {mapper_index+1} completed successfully.")
                logging.info("Mapper %d responded: SUCCESS", mapper_index)
                return True
        
        # Fault Tolerance: Scenario 2
        except grpc.RpcError as e:
            # Handle cases where the RPC call fails due to the mapper being stopped or network issues
            logging.error(f"RPC call to mapper {mapper_index+1} failed. Retrying...")
            print(f"RPC call to mapper {mapper_index + 1} failed. Retrying....")
            while(True):
                # print(f"Mapper {mapper_index+1} failed with error: {e}. Retrying.....")
                if self.call_mapper(mapper_index, start_index, end_index) == True:
                    break
                else:
                    return False
        
        except Exception as e: #Scenario1
            if(self.tmp>=5):
                print("making Trueee")
                self.mapper_status[mapper_index] = True
            while(True):
                if self.call_mapper(mapper_index, start_index, end_index) == True:
                    break
                else:
                    return False
        finally:
            channel.close()

    def call_reducer(self, reducer_index):
        channel = grpc.insecure_channel(self.reducer_addresses[reducer_index])
        stub = mapper_pb2_grpc.ReducerStub(channel)
        try:
            if not self.reducer_status[reducer_index]:
                logging.info("Reducer %d undergoing: SIMULATED FAILURE", reducer_index)
                self.tmp2+=1
                print(f"Reducer {reducer_index+1} failed with simulated error. Retrying.....")
                print("Sending ERRORSS")
                raise ValueError("Reducer is down. Trying next reducer123...")
            else:
                response = stub.ReduceData(mapper_pb2.ReduceRequest())
                if response.success:
                    print(f"Reducer {reducer_index+1} responded with updated centroids.")
                    logging.info("Reducer %d responded: SUCCESS", reducer_index)
                    for kv in response.updated_centroids: # updated_centroids=[(0:(x,y)), (1:(x,y))], kv=0:(x,y)
                        # Assuming each key only has one point associated
                        if kv.values: #list containing single new centroid
                            point = kv.values[0]  # Get the first point (assuming one point per key)
                            self.centroids[kv.key] = (point.x, point.y) # self.centroids[0]=(x,y)
                    self.reducer_acks+=1
                    return True
                else:
                    print(f"Reducer {reducer_index+1} failed or returned an unsuccessful response.")
                    logging.error("Reducer %d responded: FAILURE", reducer_index)

        except grpc.RpcError as e:
            print(f"RPC failed with Reducer {reducer_index+1}: Reducer Unavailable. Retrying....")
            logging.error("Reducer %d failed with error: %s", reducer_index+1, str(e))
            while(True):
                # print(f"Mapper {mapper_index+1} failed with error: {e}. Retrying.....")
                if self.call_reducer(reducer_index) == True:
                    break
                else:
                    return False

        except Exception as e: #Scenario1
            if(self.tmp2>=5):
                print("making Trueee")
                self.reducer_status[reducer_index] = True
            while(True):
                if self.call_reducer(reducer_index) == True:
                    break
                else:
                    return False
        finally:
            channel.close()

    def collect_results(self):
        """ Collect results from all reducers parallely"""
        logging.info("Collecting results from reducers...")
        threads = []
        for i in range(self.reducers):
            thread = threading.Thread(target=self.call_reducer, args=(i,))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        
    def run(self):
        if os.path.exists('Data\centroids.txt'):
            os.remove('Data\centroids.txt')
        partition_num=0
        for iteration in range(self.iterations):
            print(f"_________________Iteration{iteration+1} Starting_______________")
            self.mapper_acks=0
            self.reducer_acks=0
            self.distribute_data() # Master's RPC call to the mappers 
            # time.sleep(1)
            self.previous_centroids=self.centroids[:]
            # Once all mappers return, invoke the reducers with necesarry parameters
            if(self.mapper_acks==self.mappers):
                print(f"All mapper tasks completed for Iteration{iteration+1}, calling Reducers now")
                self.collect_results()
                if(self.reducer_acks==self.reducers):
                    print("Updated centroids from all reducers :", self.centroids)
                    logging.info("Updated centroids: %s", self.centroids)
                    logging.info("Iteration %d complete", iteration+1)
                    print(f"__________________Iteration {iteration + 1} complete__________________-")
                    
                    #Check for convergence
                    if self.has_converged():
                        logging.info("Convergence reached after %d iterations.", iteration+1)
                        print(f"Convergence reached after {iteration + 1} iterations.")
                        break
                    # time.sleep(0.2)
        self.write_output()

    def has_converged(self, epsilon=0.001):
        if not self.previous_centroids or not self.centroids: #yaad se previous_centroids ko centr change se pehle update karna hai
            return False
        for old, new in zip(self.previous_centroids, self.centroids):
            # distance = sum((o-n) ** 2 for o,n in zip(old, new)) **0.5
            distance = math.sqrt(sum((x - y) ** 2 for x, y in zip(new, old)))
            # print(distance)
            if distance>epsilon: #not converged
                return False
        print("Final centroids")
        for i in self.centroids:
            print(i)
        return True

    def write_output(self):
        with open('Data\centroids.txt', 'w') as file:
            for centroid in self.centroids:
                file.write(f"({centroid[0]},{centroid[1]})\n")
  
if __name__ == "__main__":
    data_points = []
    num_centroids=2
    # Load data points from file
    with open('Data\Input\points2.txt', 'r') as file:
        for line in file:
            x, y = map(float, line.strip().split(','))
            tmp=[]
            tmp.append(x)
            tmp.append(y)
            data_points.append(tmp)
    # Check if we have enough data points
    if num_centroids > len(data_points):
        print("Number of centroids requested exceeds the number of available data points.")
    # Randomly select initial centroids
    centroids = random.sample(data_points, num_centroids) #random k centroids from input.txt
    # centroids = data_points[:num_centroids] #first k centroid of input.txt
    print(f"centroids areeee: {centroids}")

    # centroids=[[0,0],[0,5],[7,7]]
    # centroids=[[8.5,2.7],[10.3,-0.3],[9.7,0.8]]
    # centroids = [[1.4302438269507878, 2.4735967228568025], [-6.051219043568199, 1.092345773013907]] #points3
    # centroids = [[81.15927937347692, 57.750360036814705], [31.557122052707076, 36.90041258086754], [46.66353476332117, 42.536829675995506]] #points2
    iterations= 2
    mapper_addresses = ["localhost:50051", "localhost:50052", "localhost:50053"] #, "localhost:50053"]
    reducer_addresses = ['localhost:60051', 'localhost:60052','localhost:60053','localhost:60054','localhost:60055']
    mappers= len(mapper_addresses)
    reducers= len(reducer_addresses)
    master = Master(mappers, reducers, centroids, iterations, mapper_addresses, reducer_addresses)
    master.run()