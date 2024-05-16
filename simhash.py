import hashlib
from collections import defaultdict
from tokenizer import computeWordFrequencies

def create_simhash(features):
    # Initialize an array to hold the weights for each bit position.
    simhash = [0] * 32
    
    # Compute word frequencies.
    frequencies = defaultdict(int)
    computeWordFrequencies(features, frequencies)

    for feature in features:
        # Get the hash value.
        hashed_feature = hashlib.md5(feature.encode()).hexdigest()
        # Get weights to the feature based on token frequency.
        weight = frequencies.get(feature, 0)

        # Convert the hashed feature to a binary string.
        binary_hash = bin(int(hashed_feature, 16))[2:].zfill(128)
        
        # Update the simhash value based on the hashed feature and its weight.
        for j in range(0, 32):
            if binary_hash[j] == '1':
                simhash[j] += weight
            else:
                simhash[j] -= weight

    # Convert the simhash array to a binary string.
    simhash_binary = ''.join(['1' if val > 0 else '0' for val in simhash])
    
    # Convert the binary string to an integer.
    simhash_value = int(simhash_binary, 2)
    return simhash_value

# Compute the Hamming distance between two simhash values.
def hamming_distance(simhash1, simhash2):
    return bin(simhash1 ^ simhash2).count('1') / 32.0