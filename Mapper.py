import grpc
from concurrent import futures
import mapper_pb2
import mapper_pb2_grpc
import sys
import os
import shutil  # Import shutil for safe directory removal

class Mapper(mapper_pb2_grpc.MapperServicer):

    def __init__(self, mapper_number):
        # Ensure centroids are tuples for easier distance calculations
        self.centroids = []
        self.file_path = 'Data\Input\points2.txt'
        self.start_index = 0
        self.end_index = 0
        self.mapper_number = mapper_number
        self.output_dir = f'Data\Mappers\M{self.mapper_number}'  # Directory for this mapper
        self.num_mappers=0
        self.num_reducers=0
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)  # Remove directory if it already exists
        os.makedirs(self.output_dir, exist_ok=True)  # Ensure directory exists


    def read_data(self):
        data = []
        with open(self.file_path, 'r') as file:
            for i, line in enumerate(file):
                if i >= self.start_index and i < self.end_index:
                    data.append(tuple(map(float, line.strip().split(','))))
                if i >= self.end_index:
                    break
        return data

    def ProcessData(self, request, context):
        self.centroids = [(c.x, c.y) for c in request.centroids]
        self.start_index = request.start_index
        self.end_index = request.end_index
        data = self.read_data()
        self.num_mapper=request.num_mappers
        self.num_reducers=request.num_reducers
        output = []
        for point in data:
            nearest_index = min(enumerate(self.centroids), key=lambda c: (c[1][0] - point[0]) ** 2 + (c[1][1] - point[1]) ** 2)[0]
            output.append((nearest_index, point))
        # Partition data after processing
        self.partition_data(output)

        return mapper_pb2.MapResponse(success=True)
    
    def partition_data(self, output):
        
        # for i in range(self.num_reducers):
        #     partition_path = os.path.join(self.output_dir, f'partition_{i+1}.txt')
        #     if os.path.exists(partition_path):
        #         os.remove(partition_path)

        # Create files for each reducer
        # partition_files = [open(os.path.join(self.output_dir, f'partition_{i+1}.txt'), 'a') for i in range(self.num_reducers)]
        partition_files = [open(os.path.join(self.output_dir, f'partition_{i+1}.txt'), 'w') for i in range(self.num_reducers)]

        # Write each key-value pair to the appropriate partition file
        for key, value in output:
            partition_index = key % self.num_reducers
            partition_files[partition_index].write(f"{key}:{value}\n")

        # Close all files
        for file in partition_files:
            file.close()

    def GetPartition(self, request, context):
        # Construct the path to the partition file for the requested reducer
        partition_path = os.path.join(self.output_dir, f'partition_{request.reducer_id}.txt')

        # Create a dictionary to aggregate points by key
        data_dict = {}

        # Read the partition file and aggregate data by key
        try:
            with open(partition_path, 'r') as file:
                for line in file:
                    if line.strip():  # Ensure the line is not empty
                        key, point_str = line.split(':')
                        x, y = map(float, point_str.strip()[1:-1].split(','))
                        if int(key) not in data_dict:
                            data_dict[int(key)] = []
                        data_dict[int(key)].append(mapper_pb2.Point(x=x, y=y))
            
        except FileNotFoundError:
            print(f"Partition file not found: {partition_path}")
            return mapper_pb2.PartitionResponse()  # Return empty response if file not found
        except Exception as e:
            print(f"An error occurred while reading the partition file: {e}")
            return mapper_pb2.PartitionResponse()  # Return empty response on error

        # Prepare the response with data grouped by key
        response = mapper_pb2.PartitionResponse()
        for key, points in data_dict.items():
            key_value = mapper_pb2.KeyValue()
            key_value.key = key
            key_value.values.extend(points)
            response.data.append(key_value)

        return response
    
# def GetPartition(self, request, context):
#         # Construct the path to the partition file for the requested reducer
#         partition_path = os.path.join(self.output_dir, f'partition_{request.reducer_id}.txt')

#         # Create a dictionary to aggregate points by key
#         data_dict = {}

#         # Read the partition file and aggregate data by key
#         try:
#             with open(partition_path, 'r') as file:
#                 for line in file:
#                     if line.strip():  # Ensure the line is not empty
#                         key, point_str = line.split(':')
#                         x, y = map(float, point_str.strip()[1:-1].split(','))
#                         if int(key) not in data_dict:
#                             data_dict[int(key)] = []
#                         data_dict[int(key)].append(mapper_pb2.Point(x=x, y=y))
            
#         except FileNotFoundError:
#             print(f"Partition file not found: {partition_path}")
#             return mapper_pb2.PartitionResponse()  # Return empty response if file not found
#         except Exception as e:



def serve(mapper_number):
    port = 50050 + int(mapper_number)  # Calculate port based on mapper number
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapper_pb2_grpc.add_MapperServicer_to_server(Mapper(mapper_number), server)
    server.add_insecure_port(f'[::]:{port}')
    print(f"Starting mapper {mapper_number} on port {port}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        serve(int(sys.argv[1]))
    else:
        print("Usage: python mapper.py <mapper_number>")