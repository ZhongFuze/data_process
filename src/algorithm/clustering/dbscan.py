#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2023-09-07 16:13:48
LastEditors: Zella Zhong
LastEditTime: 2023-09-21 23:41:13
FilePath: /data_process/src/algorithm/clustering/dbscan.py
Description: DBSCAN Clustering
'''
import os
import math
import time
import pandas as pd
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D



def parse_row(row):
    platform, identity, embedding_str = row.split(',', 2)
    id_ = platform + "," + identity
    # id_, embedding_str = row.split('\t', 1)
    embedding = list(map(float, embedding_str.split()))
    return [id_] + embedding


def read_file(file_path):
    data = []
    with open(file_path, "r", encoding="utf-8") as f:
        data = [parse_row(row.strip()) for row in f]
    return data

def dbscan(input_file, output_file):
    # Step 1: Reading the file
    data = read_file(input_file)
    df = pd.DataFrame(data, columns=['id', 'dim0', 'dim1', 'dim2', 'dim3', 'dim4', 'dim5', 'dim6', 'dim7', 'dim8', 'dim9'])
    # Step 2: DBSCAN Clustering
    X = df.iloc[:, 1:]
    model = DBSCAN(eps=0.001, min_samples=20)
    # model = DBSCAN(eps=0.3, min_samples=20)
    df['cluster_id'] = model.fit_predict(X)

    # Step 3: Output the file in format: id, cluster_id
    output_data = df[['id', 'cluster_id']]
    output_data.to_csv(output_file, sep="\t", index=False)

    # Identify unique cluster IDs excluding -1
    unique_clusters = list(df['cluster_id'].unique())
    unique_clusters.remove(-1) # Remove -1 from the list

    subset = df[df['cluster_id'] == 1]['id']
    subset.to_csv(output_file, sep="\t", index=False)

    # # Step 5: Pie chart representation
    # cluster_counts = df['cluster_id'].value_counts()
    
    # # You can decide whether to include the noise cluster (-1) in your pie chart
    # # If you wish to exclude the noise cluster, uncomment the next line
    # cluster_counts = cluster_counts.drop(-1)

    # labels = cluster_counts.index
    # sizes = cluster_counts.values

    # fig1, ax1 = plt.subplots()
    # ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    # ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    # plt.title('Distribution of points among DBSCAN clusters')
    # plt.show()

    # # Step 4: Plot the 3D scatter plot
    # fig = plt.figure()
    # ax = fig.add_subplot(111, projection='3d')
    # # Create a color map
    # # color_map = plt.cm.get_cmap('viridis', len(unique_clusters) - 1)
    # color_map = plt.cm.get_cmap('nipy_spectral', len(unique_clusters) - 1)

    # for cluster_id in unique_clusters:
    #     subset = df[df['cluster_id'] == cluster_id]
    #     cluster_count = len(subset)
    #     if cluster_id == 0:
    #         color = 'pink'
    #     # elif cluster_id == 1:
    #     #     color = 'red'
    #     else:
    #         color = color_map(cluster_id / (len(unique_clusters) - 1))
        
    #     ax.scatter(subset['dim0'], subset['dim1'], subset['dim2'], c=color, label=f'Cluster {cluster_id} count={cluster_count}', s=20)
    #     # ax.scatter(subset['dim3'], subset['dim4'], subset['dim5'], c=color, label=f'Cluster {cluster_id}', s=20)

    # # Adding legend at the left
    # ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1))

    # # Adjust layout to make room for the legend
    # plt.subplots_adjust(right=0.7)

    # # Save the plot
    # plt.savefig('3D_scatter.png')
    # plt.show()



def dbscan_test():
    input_file = "/Users/fuzezhong/Documents/GitHub/nextdotid/data_process/data/lens.20230716.fastrp.embedding.logN_iter30"
    output_file = "/Users/fuzezhong/Documents/GitHub/nextdotid/data_process/data/lens.20230716.fastrp.embedding.logN_iter30.dbscan_c1"
    dbscan(input_file, output_file)


if __name__ == '__main__':
    dbscan_test()
