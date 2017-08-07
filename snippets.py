# Oliver Evans
# August 7, 2017

# Random potentially useful snippets.

## DAG Generation ##
def rand_dag(n):
    "Generate random DAG adjacency matrix."
    return np.tril(np.random.randint(0, 2, [n, n]), k=-1)

def gen_bq_dag(adj_mat):
    "Generate bqplot Graph object of DAG given adjacency matrix."
    
    nxg = nx.from_numpy_matrix(adj_mat)

    pos = nx.nx_pydot.graphviz_layout(nxg, prog='dot')
    x, y = np.array([pos[i] for i in range(N)]).T

    link_data = [{'source': source, 'target': target} for source, target in nxg.edges()]

    graph = Graph(
        node_data=node_data,
        link_data=link_data,
        scales=scales,
        link_type='line',
        highlight_links=False,
        x=x, y=y
    )
    
    return nxg, graph

fig_layout = ipw.Layout(width='600px', height='800px')
adj_mat = rand_dag(N)
nxg, graph = gen_bq_dag(adj_mat)
f = bq.Figure(marks=[graph], layout=fig_layout)
f

