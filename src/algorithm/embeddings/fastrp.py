#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-09-07 16:57:27
LastEditors: Zella Zhong
LastEditTime: 2023-09-21 18:22:26
FilePath: /data_process/src/algorithm/embeddings/fastrp.py
Description: fastRP
'''
import networkx as nx
import pandas as pd
import numpy as np
from collections import namedtuple
from sklearn.random_projection import GaussianRandomProjection
from sklearn.preprocessing import normalize
import math


Dim_Tuple = namedtuple('Dim_Tuple', ['min_dim', 'max_dim', 'weight'])


def fast_random_projection(input_file,
                           output_file,
                           iteration_weights="1",
                           beta=0.5,
                           embedding_dimension=10,
                           sampling_constant=3,
                           random_seed=42,
                           print_results=False):
    '''
    description: fast random projection
    iteration_weights:
      A comma-separated string of numbers to weight each iteration of the embedding process.
    beta:
      A float value that normalizes high-degree vertices in the graph. Usually between -1 and 0, where
      -1 penalizes high-degree vertices heavily. If desired, one can use a positive value to weight
      high-degree vertices more than low-degree vertices.
    embedding_dimension:
      The total dimension of the desired embedding.
    default_weight:
      Influence that the default edge types have in the region of the embedding they are incorporated. Defaults to 1.
    embedding_dim_map:
      Set of comma-separated tuples of '<edge type>,<length>,<weight>,<starting index>'. Use if incorporating specific edge types
      into certain regions of the embedding is desired. If the empty set is used, all edge types will be considered "default" edge types.
    sampling_constant:
      Parameter defined from the FastRP paper. Controls the sparsity of the embedding. Usually an integer between 1 (fully dense embedding)
      and 3 works well.
    random_seed:
      Seed for random number generator. Defaults to 42.
    return {*}
    '''
    # dim_tuple_instance = Dim_Tuple(min_dim=1, max_dim=10, weight=0.5)
    # feature_dim_map = {}  # Map<STRING, Dim_Tuple>
    # embedding_dim_map = {} # Map<STRING, Dim_Tuple>
    # Loading the data
    data = pd.read_csv(input_file, sep='\t', header=0, names=['from', 'to', 'action'])
    # Create a directed graph from the data
    G = nx.from_pandas_edgelist(data, 'from', 'to', edge_attr=['action'], create_using=nx.DiGraph())
    # Calculate out-degree for each node
    outdegrees = dict(G.out_degree())

    # Total edge count
    m = G.number_of_edges()

    # Extract unique nodes from the 'from' column
    unique_nodes = data['from'].unique()

    # Compute L for each unique node based on its outdegree and the formula provided
    L_values = [np.power(outdegrees[node] / m, beta) for node in unique_nodes]
    # Constructing normalized diagonal elements of inverse degree matrix L
    # Convert L_values to a numpy array
    L = np.array(L_values)

    N = len(G.nodes())
    embeddings = np.zeros((N, embedding_dimension))
    final_embeddings = np.zeros((N, embedding_dimension))

    # Initializations
    _mod = 2**31 - 1
    _mult = 1664525
    _inc = 1013904223
    v1 = math.sqrt(sampling_constant)
    v2 = -v1
    v3 = 0.0
    p1 = 0.5 / sampling_constant
    p2 = p1
    p3 = 1 - 1.0 / sampling_constant

    node_idx_map = {node: idx for idx, node in enumerate(G.nodes())}

    # Randomly initialize embeddings
    for node in G.nodes():
        for neighbor in G.neighbors(node):
            inc = (node_idx_map[node] + _inc)

    for node in G.nodes():
        for neighbor in G.neighbors(node):
            inc = (node_idx_map[node] + _inc)
            r = ((inc + _mult * random_seed) % _mod)
            mr = r / (_mod * 1.0)

            if mr <= p1 + 0.0001:
                embeddings[node_idx_map[neighbor]] += v1 * L[node_idx_map[node]]
            elif mr <= p1 + p2 + 0.0001:
                embeddings[node_idx_map[neighbor]] += v2 * L[node_idx_map[node]]
            else:
                embeddings[node_idx_map[neighbor]] += v3 * L[node_idx_map[node]]

    # Main Execution
    weights = [float(i) for i in iteration_weights.split(",")]
    for weight in weights:
        for node in G.nodes():
            for neighbor in G.neighbors(node):
                embeddings[node_idx_map[neighbor]] += embeddings[node_idx_map[node]]
                
            # POST-ACCUM
            square_sum = 0
            out = max([1.0, G.degree(node)])
            for total in embeddings[node_idx_map[node]]:
                square_sum += pow(total / out, 2)
            square_sum = math.sqrt(square_sum)
            for i in range(embedding_dimension):
                if abs(square_sum) < 0.00001:
                    break
                final_embeddings[node_idx_map[node]][i] = embeddings[node_idx_map[node]][i] / out / square_sum * weight
                value = embeddings[node_idx_map[node]][i] / out / square_sum
                embeddings[node_idx_map[node]][i] = -embeddings[node_idx_map[node]][i] + value

    # Saving the embeddings to output file
    with open(output_file, 'w') as f:
        for i, node in enumerate(G.nodes()):
            embedding_str = ' '.join(map(str, final_embeddings[i]))
            f.write(f"{node} {embedding_str}\n")
    
    if print_results:
        print(final_embeddings)
    



def gsrp(input_file, output_file):
    # FastRP [Che+19] uses sparse Gaussian random projections
    # Step 1: Read the data and create a graph
    data = pd.read_csv(input_file, sep='\t', header=0, names=['from', 'to'])
    G = nx.from_pandas_edgelist(data, 'from', 'to', create_using=nx.Graph())

    # Step 2: Create an adjacency matrix
    adj_matrix = nx.adjacency_matrix(G).toarray()

    # Step 3: Implement Random Projection
    transformer = GaussianRandomProjection(n_components=10) # Adjust the number of components as needed
    low_dim_matrix = transformer.fit_transform(adj_matrix)

    # Step 4: Create a dictionary of embeddings
    node_list = list(G.nodes)
    embeddings = {node: low_dim_matrix[i] for i, node in enumerate(node_list)}

    # Step 5: Save the embeddings to a file
    with open(output_file, 'w', encoding="utf-8") as f:
        for node_id, embedding in embeddings.items():
            embedding_str = ' '.join(map(str, embedding))
            f.write(f"{node_id}\t{embedding_str}\n")

    print("Embeddings have been saved to %s" % output_file)


def test_gsrp():
    input_file = "/Users/fuzezhong/Documents/GitHub/nextdotid/data_process/data/lens.uniquesocial.tsv"
    output_file = "/Users/fuzezhong/Documents/GitHub/nextdotid/data_process/data/lens.gsrpembeding.tsv"
    gsrp(input_file, output_file)


def test_fastrp():
    input_file = "/Users/fuzezhong/Documents/GitHub/nextdotid/data_process/data/lens.uniquesocial.tsv"
    output_file = "/Users/fuzezhong/Documents/GitHub/nextdotid/data_process/data/lens.fastrp.embeding.tsv"
    iteration_weights = "0.109151,0.068867,0.054576,0.047009,0.042226,0.038881,0.036384,0.034433,0.032858,0.031552,0.030447,0.029497,0.028669,0.027938,0.027288,0.026704,0.026176,0.025695,0.025255,0.024851,0.024477,0.02413,0.023806,0.023504,0.023222,0.022956,0.022705,0.022468,0.022245,0.022030000000000164"
    fast_random_projection(input_file, output_file, iteration_weights)


if __name__ == '__main__':
    test_fastrp()
