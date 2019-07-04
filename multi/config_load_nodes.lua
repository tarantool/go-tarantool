local function get_cluster_nodes()
    return { 'localhost:3013', 'localhost:3014' }
end

return {
    get_cluster_nodes = get_cluster_nodes
}