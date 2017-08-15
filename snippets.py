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

rte_html = r"""
            <h1>Radiative Transfer</h1>

            The Radiative Transfer Equation is given by

            <p>
            $$\nabla I \cdot \omega = -c\, I(x, \omega) + \int_\Omega \beta(|\omega-\omega'|)\, I(x, \omega')$$
            </p>

            It is useful for
            <ul>
            <li>
            Stellar astrophysics
            </li>
            <li>
            Kelp
            </li>
            <li>
            Nice conversations
            </li>
            </ul>

            And is explained well by the following diagram.
            <br />
            <br />
            <img width=300px src="http://soap.siteturbine.com/uploaded_files/www.oceanopticsbook.info/images/WebBook/0dd27b964e95146d0af2052b67c7b5df.png" />
        """
