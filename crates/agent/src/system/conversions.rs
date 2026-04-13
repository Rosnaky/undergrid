use mesh::undergrid::ResourceSnapshot;

use crate::system::{CpuInfo, DiskInfo, MemoryInfo, SystemSnapshot};

impl From<ResourceSnapshot> for SystemSnapshot {
    fn from(r: ResourceSnapshot) -> Self {
        SystemSnapshot {
            cpu: CpuInfo {
                cpu_cores: r.cpu_cores as usize,
                cpu_usage_pct: r.cpu_usage_pct,
                cpu_freq_mhz: 0.0,
            },
            memory: MemoryInfo {
                memory_total_bytes: r.memory_total_bytes,
                memory_available_bytes: r.memory_available_bytes,
            },
            disk: DiskInfo {
                disk_total_bytes: r.disk_total_bytes,
                disk_available_bytes: r.disk_available_bytes,
            },
            gpu: None,
            hostname: String::new(),
        }
    }
}
