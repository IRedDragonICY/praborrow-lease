use petgraph::graph::DiGraph;
use std::collections::HashMap;

/// Node ID in the wait-for graph (hashed Sovereign ID or Holder ID).
type NodeId = u128;

/// A directed graph representing "Process A is waiting for Resource B".
/// 
/// - Nodes: Resource IDs and Lease Holder IDs.
/// - Edges: 
///   - (Holder -> Resource): Holder requests lease.
///   - (Resource -> Holder): Resource is currently leased by Holder.
#[derive(Debug, Default)]
pub struct WaitForGraph {
    /// The underlying graph structure.
    graph: DiGraph<NodeId, ()>,
    /// Map to quickly look up graph indices.
    node_map: HashMap<NodeId, petgraph::graph::NodeIndex>,
}

impl WaitForGraph {
    /// Creates a new, empty wait-for graph.
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_map: HashMap::new(),
        }
    }

    /// Records a new dependency: `who` is waiting for `what`.
    pub fn add_wait(&mut self, who: NodeId, what: NodeId) {
        let who_idx = *self.node_map.entry(who).or_insert_with(|| self.graph.add_node(who));
        let what_idx = *self.node_map.entry(what).or_insert_with(|| self.graph.add_node(what));
        
        // Add edge if not already present (simplified mainly for stub)
        self.graph.update_edge(who_idx, what_idx, ());
    }

    /// Removes a dependency when a resource is acquired or request cancelled.
    pub fn remove_wait(&mut self, who: NodeId, what: NodeId) {
        if let (Some(&who_idx), Some(&what_idx)) = (self.node_map.get(&who), self.node_map.get(&what)) {
             if let Some(edge) = self.graph.find_edge(who_idx, what_idx) {
                 self.graph.remove_edge(edge);
             }
        }
    }

    /// Detects cycles in the graph (Deadlocks).
    /// Returns true if a cycle exists.
    pub fn detect_cycle(&self) -> bool {
        petgraph::algo::is_cyclic_directed(&self.graph)
    }
}

/// Service that monitors lease contentions.
pub struct DeadlockDetector {
    graph: WaitForGraph,
}

impl DeadlockDetector {
    pub fn new() -> Self {
        Self {
            graph: WaitForGraph::new(),
        }
    }

    pub fn check_deadlock(&self) -> bool {
        self.graph.detect_cycle()
    }
}
