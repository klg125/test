import sys
from collections import defaultdict

def map_function(line):
    substrings = ["nu", "chi", "haw"]
    output = []
    line_split = line.split()
    if len(line_split) == 4:
        word = line_split[0]
        year = line_split[1]
        volumes = line_split[3]
    elif len(line_split) == 5:
        word1 = line_split[0]
        word2 = line_split[1]
        year = line_split[2]
        volumes = line_split[4]
    else:
        return output  # Incorrectly formatted line

    try:
        year_int = int(year)
        volumes_int = int(volumes)
    except ValueError:
        return output  # Year or volumes not an integer

    if not (0 < year_int <= 2022):
        return output  # Year out of valid range

    for substring in substrings:
        count = 0
        if len(line_split) == 4 and word.count(substring):
            count = 1
        elif len(line_split) == 5:
            if word1.count(substring):
                count += 1
            if word2.count(substring):
                count += 1

        if count > 0:
            output.append(((year_int, substring), (volumes_int * count, count)))

    return output

def reduce_function(key, values):
    total_volumes = sum(v[0] for v in values)
    total_counts = sum(v[1] for v in values)
    if total_counts > 0:
        return (key, total_volumes / total_counts)
    else:
        return (key, 0)

def main():
    input_data = sys.stdin.read().splitlines()
    intermediate = defaultdict(list)
    
    # Simulate the Map phase
    for line in input_data:
        output = map_function(line)
        for key, value in output:
            intermediate[key].append(value)
    
    # Simulate the Reduce phase
    final_results = []
    for key, values in intermediate.items():
        result = reduce_function(key, values)
        final_results.append(result)
    
    # Print the final results
    for (year, substring), avg in sorted(final_results):
        print(f"{year},{substring},{avg:.2f}")

if __name__ == "__main__":
    main()
