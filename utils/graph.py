import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt

# # Group reviews by product_ID to find reviewers for each product
# product_reviewers = filtered_review_df.groupby('product_ID')['reviewer_ID'].apply(list).reset_index()

# # Create an empty graph
# G = nx.Graph() #directly create gives tohuge graph

# # Add nodes (reviewers)
# all_reviewers = filtered_review_df['reviewer_ID'].unique()
# G.add_nodes_from(all_reviewers)

# # Add edges between reviewers who purchased the same product
# edge_list = []

# for _, row in product_reviewers.iterrows():
#     reviewers = row['reviewer_ID']
#     # Create edges between all pairs of reviewers for this product
#     for i in range(len(reviewers)):
#         for j in range(i+1, len(reviewers)):
#             edge_list.append((reviewers[i], reviewers[j]))

# # Add all edges to the graph
# G.add_edges_from(edge_list)

# print(f"Network created with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")

# # Visualize the network (for small networks only)
# if G.number_of_nodes() < 1000:  # Only visualize if network is small
#     plt.figure(figsize=(10, 10))
#     pos = nx.spring_layout(G, k=0.3)  # positions for all nodes
#     nx.draw(G, pos, node_size=50, node_color='blue', alpha=0.6)
#     plt.title("Reviewer Network")
#     plt.show()
# else:
#     print("Network too large to visualize. Showing statistics instead:")
#     print(f"Average degree: {2*G.number_of_edges()/G.number_of_nodes():.2f}")
#     print(f"Number of connected components: {nx.number_connected_components(G)}")
    
#     # Show largest connected component size
#     largest_cc = max(nx.connected_components(G), key=len)
#     print(f"Size of largest connected component: {len(largest_cc)}")