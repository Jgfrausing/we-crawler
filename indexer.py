import hashlib
import re
from urllib.request import urlopen

global_hash_functions = [hashlib.md5, hashlib.sha1, hashlib.sha224, hashlib.sha256, hashlib.sha384, hashlib.sha512]


def get_shingles(str_input):
    str_input = str_input.lower()
    parts = str_input.split(" ")
    shingles = []

    # We are using 4-shingles (therefore len(parts) - 3)
    for part in range(0, len(parts) - 3):
        shingles.append(parts[part] + " " + parts[part + 1] + " " + parts[part + 2] + " " + parts[part + 3])

    return shingles


def get_min_hash(shingles, hash_func):
    min_hash_value = hash_func(shingles[0].encode()).hexdigest()
    for shingle in range(1, len(shingles)):
        temp_hash_value = hash_func(shingles[shingle].encode()).hexdigest()
        if temp_hash_value < min_hash_value:
            min_hash_value = temp_hash_value

    return min_hash_value


def get_hash_values_from_shingles(shingles):
    # noinspection PyListCreation
    hash_values = []

    for hash_function in range(0, len(global_hash_functions)):
        hash_values.append(get_min_hash(shingles, global_hash_functions[hash_function]))

    return hash_values


def combine_hash_values(start_value, length, hash_values):
    return_value = ""
    for hash_value in range(start_value, start_value + length):
        return_value += str(hash_values[hash_value])

    return global_hash_functions[0](return_value.encode()).hexdigest()


def get_hash_values_from_text(text):
    shingles = get_shingles(text)
    hash = get_hash_values_from_shingles(shingles)
    return hash


def compare_hashes(hash1, hash2):
    # compare pairwise hash values
    intersection = 0
    for x in range(0, len(global_hash_functions)):
        if hash1[x] == hash2[x]:
            intersection += 1

    return str(intersection / ((2 * len(global_hash_functions)) - intersection))


######## No used ########
def get_hash_super_shingles(hash_values):
    supers = []
    super_size = 3
    length = super_size

    for hash_function in range(0, 1 + len(global_hash_functions) // super_size):
        if super_size * hash_function + length >= len(global_hash_functions):
            length = len(global_hash_functions) - (super_size * (hash_function - 1) + length)
        supers.append(combine_hash_values(hash_function * super_size, length, hash_values))

    return supers