use crate::models::tasks::{TaskPriority, TaskRouteInfo};

pub fn route_task(priority: &TaskPriority) -> TaskRouteInfo {
    match priority {
        TaskPriority::Low => TaskRouteInfo {
            node_type: "edge_node".to_string(),
            node_label: "Zephost Edge 03".to_string(),
            estimated_cost: 3.0,
        },
        TaskPriority::High => TaskRouteInfo {
            node_type: "cloud_vm".to_string(),
            node_label: "Zephost Cloud 07".to_string(),
            estimated_cost: 15.0,
        },
        TaskPriority::Enterprise => TaskRouteInfo {
            node_type: "dedicated_cluster".to_string(),
            node_label: "Zephost Core 12".to_string(),
            estimated_cost: 50.0,
        },
    }
}
