import csv
import os
import sys
from multiprocessing import Process, Queue
import io

def read_from_file(file_path):
    """ Read data from a file and return as a list of lines. """
    with open(file_path, 'r', newline='') as file:
        return file.readlines()

def split_data(data, num_splits):
    """ Splits the data into smaller files. """
    header = data[0]
    num_lines = len(data)
    chunk_size = num_lines // num_splits
    
    # Create a directory to store the split files
    if not os.path.exists('split_data'):
        os.makedirs('split_data')
    
    for i in range(num_splits):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i < num_splits - 1 else num_lines
        split_data = data[start_index:end_index]
        with open(f'split_data/split_{i}.csv', 'w', newline='') as f:
            f.write(header)
            f.writelines(split_data)

def map_function(data_file, output_queue):
    """ Processes the data from a file to map artist to song durations. """
    with open(data_file, 'r', newline='') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            artist = row[2]
            duration = float(row[3])
            output_queue.put((artist, duration))

def shuffle(queue, num_reducers):
    """ Shuffles mapper outputs to reducer-specific queues. """
    shuffle_queues = [Queue() for _ in range(num_reducers)]
    while not queue.empty():
        artist, duration = queue.get()
        index = hash(artist) % num_reducers
        shuffle_queues[index].put((artist, duration))
    return shuffle_queues

def reduce_function(input_queue, output_queue):
    """ Reduces mapped values to find the maximum song duration per artist. """
    max_duration = {}
    while not input_queue.empty():
        artist, duration = input_queue.get()
        if artist in max_duration:
            if duration > max_duration[artist]:
                max_duration[artist] = duration
        else:
            max_duration[artist] = duration
    for artist, duration in max_duration.items():
        output_queue.put((artist, duration))

def main(num_splits, num_maps, num_reduces):
    """ Main function to execute map-reduce operations with split files. """
    for i in range(num_splits):
        split_file = f'split_data/split_{i}.csv'
        map_queue = Queue()
        map_processes = [Process(target=map_function, args=(split_file, map_queue)) for _ in range(num_maps)]
        for p in map_processes:
            p.start()
        for p in map_processes:
            p.join()

        shuffle_queues = shuffle(map_queue, num_reduces)
        reduce_queue = Queue()
        reduce_processes = [Process(target=reduce_function, args=(q, reduce_queue)) for q in shuffle_queues]
        for p in reduce_processes:
            p.start()
        for p in reduce_processes:
            p.join()

        while not reduce_queue.empty():
            print(reduce_queue.get())

if __name__ == "__main__":
    num_splits = 20
    num_maps = int(sys.argv[1])
    num_reduces = int(sys.argv[2])

    # Split the input data into 20 files
    input_data = read_from_file(sys.stdin.fileno())
    split_data(input_data, num_splits)

    # Perform map-reduce operations on the split files
    main(num_splits, num_maps, num_reduces)
