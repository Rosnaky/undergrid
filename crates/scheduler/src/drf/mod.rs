/*
Dominant Resource Fairness scheduler
https://people.eecs.berkeley.edu/~alig/papers/h-drf.pdf
*/

use std::cmp::Reverse;

use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use runtime::task::{TaskId, TaskSpec};

use crate::{Assignment, NodeResources, Scheduler, scheduler_error::SchedulerError};

pub struct DrfScheduler;

impl Scheduler for DrfScheduler {
    fn schedule(
        &self,
        tasks: &[&TaskSpec],
        nodes: &[NodeResources],
    ) -> Result<(Vec<Assignment>, Vec<SchedulerError>), SchedulerError> {
        // Get total cluster resources
        let mut total_available_cpu = 0.0;
        let mut total_available_memory_bytes = 0;
        let mut total_available_disk_bytes = 0;
        let mut total_gpus = 0;

        for nr in nodes {
            total_available_cpu += nr.available_cpu;
            total_available_memory_bytes += nr.available_memory_bytes;
            total_available_disk_bytes += nr.available_disk_bytes;
            total_gpus += nr.available_gpu as u32;
        }

        // Compute dominant share
        let mut dominant_shares: PriorityQueue<TaskId, Reverse<OrderedFloat<f64>>> = PriorityQueue::new();

        for task in tasks {
            let max_dom_resource = 0.0_f64
                .max(task.resources.cpu_cores / total_available_cpu)
                .max(task.resources.memory_bytes as f64 / total_available_memory_bytes as f64)
                .max(task.resources.disk_bytes as f64 / total_available_disk_bytes as f64)
                .max(
                    if total_gpus != 0 { task.resources.gpu as u32 as f64 / total_gpus as f64 }
                    else { 0.0 }
                );
            dominant_shares.push(task.id.clone(), Reverse(OrderedFloat(max_dom_resource)));
        }

        let mut assignments: Vec<Assignment> = Vec::new();
        let mut scheduler_errors: Vec<SchedulerError> = Vec::new();
        let mut transformed_nodes: Vec<NodeResources> = nodes.to_vec();
        
        // Schedule task with smallest dominant share first using best fit placement
        for (task_id, _) in dominant_shares.clone().into_sorted_iter() {
            match self.get_best_fit(
                tasks.iter().find(|task| task.id == task_id).unwrap(),
                &mut transformed_nodes,
            ) {
                Ok(assignment) => {
                    // Subtract resources
                    if let Some(node) = transformed_nodes.iter_mut().find(|n| n.node_id == assignment.node_id) {
                        let task = tasks.iter().find(|t| t.id == task_id).unwrap();
                        node.available_cpu -= task.resources.cpu_cores;
                        node.available_memory_bytes -= task.resources.memory_bytes;
                        node.available_disk_bytes -= task.resources.disk_bytes;
                        node.available_gpu -= task.resources.gpu as u32;
                    }

                    assignments.push(assignment);
                }
                Err(e) => scheduler_errors.push(e),
            }
        }

        Ok((assignments, scheduler_errors))
    }    
}

impl DrfScheduler {
    /// Pick node with least remaining resources after placement
    fn get_best_fit(
        &self,
        task: &TaskSpec,
        nodes: &[NodeResources],
    ) -> Result<Assignment, SchedulerError> {
        let mut remaining_resources = 1.0;
        let mut node_id: Option<String> = None;

        for node in nodes {
            let min_resource_remaining = 100.0_f64
                .min((node.available_cpu - task.resources.cpu_cores) / node.available_cpu)
                .min((node.available_memory_bytes as f64 - task.resources.memory_bytes as f64)/node.available_memory_bytes as f64)
                .min((node.available_disk_bytes as f64 - task.resources.disk_bytes as f64)/node.available_disk_bytes as f64)
                .min(
                    if !task.resources.gpu {
                        f64::INFINITY
                    }
                    else {
                        node.available_gpu as i32 as f64 - task.resources.gpu as i32 as f64
                    }
                );

            if min_resource_remaining >= 0.0 && min_resource_remaining < remaining_resources {
                remaining_resources = min_resource_remaining;
                node_id = Some(node.node_id.clone());
            }
        }

        match node_id {
            Some(id) => {
                Ok(Assignment {
                    task_id: task.id.clone(),
                    node_id: id,
                })
            }
            _ => {
                Err(SchedulerError::InsufficientResources(format!("Insufficient resources for task {}", task.id)))
            }
        }
    }
}
