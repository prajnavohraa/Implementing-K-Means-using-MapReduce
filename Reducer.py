import grpc
from concurrent import futures
import sys
import os
import mapper_pb2
import mapper_pb2_grpc
import shutil

class Reducer(mapper_pb2_grpc.ReducerServicer):
    def __init__(self, output_file, reducer_id, num_mappers):
        self.local_data = {}  # Local store for aggregated data
        self.output_file = output_file
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        self.updated_centroids={}
        self.reducer_id = reducer_id
        self.num_mappers=num_mappers

        # self.output_dir = f'Data/Reducers/'
        # if os.path.exists(self.output_dir):
        #     shutil.rmtree(self.output_dir)
        # os.makedirs(self.output_dir, exist_ok=True)
        # if os.path.exists(self.output_file):
        #     os.remove(self.output_file)


    #RPC
    def ReduceData(self, request, context):
        # Process partition files, perform reduction, and respond with new centroids
        # self.shuffle_and_sort()  # Assume all data needed is locally available or fetched earlier
        # new_centroids = self.reduce()  # Calculate new centroids
        # updated_centroids={}
        # for key, (x, y) in self.updated_centroids.items():

        self.shuffle_and_sort(self.reducer_id, self.num_mappers) #get data from mapper through RPC
        # self.save_sorted_data() #apne liye not needed
        self.write_results() #reducer task complete so send updated centroids to Master in response
        
        response = mapper_pb2.ReduceResponse()
        response.success = True  # Indicating successful communication

        for key, centroid in self.updated_centroids.items(): #self.updated_centroids={0:(x,y), 1:(x,y)}
            kv = mapper_pb2.KeyValue()
            kv.key = int(key) #kv.key=0
            point=mapper_pb2.Point(x=centroid[0], y=centroid[1]) #point=(x,y)
            # point.x=centroid[0]
            # point.y=centroid[1]
            kv.values.append(point) #kv.values=[(x,y)]
            response.updated_centroids.append(kv) #response.updated_centroids=[(0:(x,y))]
        return response #{True, [(0:(x,y)), (1:(x,y))]}
    

    #self.updates_centroids={0:(x,y), 1:(x,y), 2:(x,y)} dict
        

    def fetch_data_from_mapper(self, mapper_id, reducer_id):
        channel = grpc.insecure_channel(f'localhost:5005{mapper_id+1}')
        stub = mapper_pb2_grpc.MapperStub(channel)
        request = mapper_pb2.PartitionRequest(reducer_id=reducer_id, mapper_id=mapper_id)
        response = stub.GetPartition(request)
        channel.close()
        return response

    def shuffle_and_sort(self, reducer_id, num_mappers):
        for mapper_id in range(num_mappers):
            key_values = self.fetch_data_from_mapper(mapper_id, reducer_id) # key_values.data = 0:[(x1,y1), (x2,y2)...], 1:[(x1,y1), (x2,y2)...]
            for key_value in key_values.data: #key_value = 0:[(x1,y1), (x2,y2)...]
                key = key_value.key #key=0
                points = key_value.values #points=[(x1,y1), (x2,y2)...]
                if key not in self.local_data: #making local copy of key_values.data
                    self.local_data[key] = []
                for point in points:
                    self.local_data[key].append((point.x, point.y)) #self.local_data={0:[(x1,y1), (x2,y2)...], 1:[(x1,y1), (x2,y2)...]}

    



    def reduce(self):
        # Calculate new centroids
        new_centroids = {}
        for key, points in self.local_data.items(): #self.local_data={0:[(x1,y1), (x2,y2)...], 1:[(x1,y1), (x2,y2)...]}
            avg_x = sum(p[0] for p in points) / len(points) #key=0, points=[(x1,y1), (x2,y2)...]
            avg_y = sum(p[1] for p in points) / len(points)
            new_centroids[key] = (avg_x, avg_y) #new_centroids={0:(x,y), 1:(x,y)}
        self.updated_centroids = new_centroids
        return new_centroids

    def write_results(self):
        with open(self.output_file, 'w') as file: #R1.txt ya R2.txt
            for key, centroid in self.reduce().items():
                file.write(f"{key}: ({centroid[0]}, {centroid[1]})\n")

    # def SendResults(self, request, context):
    #     if not os.path.exists(self.output_file):
    #         return mapper_pb2.CollectResultsResponse(success=False)

    #     results = []
    #     with open(self.output_file, 'r') as file:
    #         for line in file:
    #             key, values = line.strip().split(': ')
    #             x, y = map(float, values.strip('()').split(','))
    #             results.append(mapper_pb2.CentroidResult(key=int(key), x=x, y=y))

    #     return mapper_pb2.CollectResultsResponse(results=results, success=True)


def serve(reducer_id, output_file, num_mappers):
    port = 60050 + int(reducer_id)  # Calculate port based on reducer number, starting from 60051
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reducer = Reducer(output_file, reducer_id, num_mappers)
    
    mapper_pb2_grpc.add_ReducerServicer_to_server(reducer, server)
    server.add_insecure_port(f'[::]:{port}')
    print(f"Starting reducer {reducer_id} on port {port}")
    server.start()
    # reducer.shuffle_and_sort(int(reducer_id), num_mappers)
    # reducer.save_sorted_data()
    # reducer.write_results()

    server.wait_for_termination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python Reducer.py <reducer_id>")
        sys.exit(1)
    
    reducer_id = sys.argv[1]
    output_file = f"Data/Reducers/R{reducer_id}.txt"
    num_mappers = 3
    serve(int(reducer_id), output_file, num_mappers)
