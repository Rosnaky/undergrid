use std::fs;

use crate::system::system_error::SystemError;

#[derive(Debug, Clone)]
pub struct CpuInfo {
    pub cpu_cores: usize,
    pub cpu_usage_pct: f64,
    pub cpu_freq_mhz: f64,
}

#[derive(Debug, Clone)]
pub struct MemoryInfo {
    pub memory_total_bytes: u64,
    pub memory_available_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct DiskInfo {
    pub disk_total_bytes: u64,
    pub disk_available_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct GpuInfo {
    pub gpu_freq_mhz: f64,
    pub memory_total_bytes: u64,
    pub memory_available_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SystemSnapshot {
    pub cpu: CpuInfo,
    pub disk: DiskInfo,
    pub memory: MemoryInfo,
    pub gpu: Option<GpuInfo>,
    pub hostname: String,
}

impl SystemSnapshot {
    pub fn collect() -> Result<Self, SystemError> {
        let cpu = Self::read_cpu()?;
        let memory = Self::read_memory()?;
        let disk = Self::read_disk()?;
        let hostname = gethostname::gethostname()
            .to_string_lossy()
            .to_string();

        Ok(SystemSnapshot {
            cpu,
            memory,
            disk,
            gpu: None,
            hostname,
        })
    }

    pub fn display(&self) {
        println!("╔══════════════════════════════════════╗");
        println!("║       SYSTEM RESOURCES               ║");
        println!("╠══════════════════════════════════════╣");
        println!("║ Host:   {}", self.hostname);
        println!("║ CPU:    {} cores", self.cpu.cpu_cores);
        println!(
            "║ Memory: {} / {} MB",
            (self.memory.memory_total_bytes - self.memory.memory_available_bytes) / 1_048_576,
            self.memory.memory_total_bytes / 1_048_576
        );
        println!(
            "║ Disk:   {} / {} GB",
            (self.disk.disk_total_bytes - self.disk.disk_available_bytes) / 1_073_741_824,
            self.disk.disk_total_bytes / 1_073_741_824
        );
        println!("╚══════════════════════════════════════╝");
    }

    fn read_cpu() -> Result<CpuInfo, SystemError> {
        let cpuinfo_path = "/proc/cpuinfo";
        let contents = fs::read_to_string(cpuinfo_path)
            .map_err(|e| SystemError::ReadError(format!("{}: {}", cpuinfo_path, e)))?;

        let cpu_cores = contents
            .lines()
            .filter(|line| line.starts_with("processor"))
            .count();

        if cpu_cores == 0 {
            return Err(SystemError::ParseError(
                format!("Found 0 processors in {}", cpuinfo_path).to_string(),
            ));
        }

        let cpu_freq_mhz: f64 = contents
            .lines()
            .find(|line| line.starts_with("cpu MHz"))
            .ok_or_else(|| SystemError::ParseError("cpu MHz not found".into()))?
            .split(':')
            .nth(1)
            .ok_or_else(|| SystemError::ParseError("cpu MHz malformed".into()))?
            .trim()
            .parse()
            .map_err(|e| SystemError::ParseError(format!("Bad cpu MHz value: {}", e)))?;


        Ok(CpuInfo {
            cpu_cores,
            cpu_usage_pct: 0.0,
            cpu_freq_mhz,
        })
    }

    fn read_memory() -> Result<MemoryInfo, SystemError> {
        let meminfo_path: &str = "/proc/meminfo";
        let contents: String = fs::read_to_string(meminfo_path)
            .map_err(|e| SystemError::ReadError(format!("{}: {}", meminfo_path, e)))?;

        let mut total: Option<u64> = None;
        let mut available: Option<u64> = None;

        for line in contents.lines() {
            if line.starts_with("MemTotal:") {
                total = Some(Self::parse_meminfo_line(line)?);
            }
            else if line.starts_with("MemAvailable:") {
                available = Some(Self::parse_meminfo_line(line)?);
            }

            if total.is_some() && available.is_some() {
                break;
            }
        }

        Ok(MemoryInfo {
            memory_total_bytes: total
                .ok_or_else(|| SystemError::ParseError("MemTotal not found".into()))?,
            memory_available_bytes: available
                .ok_or_else(|| SystemError::ParseError("MemAvailable not found".into()))?,
        })
    }

    fn parse_meminfo_line(line: &str) -> Result<u64, SystemError> {
        let value_part = line
            .split(':')
            .nth(1)
            .ok_or_else(|| SystemError::ParseError(format!("Malformed line: {}", line)))?;

        let kb: u64 = value_part
            .split_whitespace()
            .next()
            .ok_or_else(|| SystemError::ParseError(format!("No value in: {}", line)))?
            .parse()
            .map_err(|e| SystemError::ParseError(format!("Bad number in '{}': {}", line, e)))?;

        Ok(kb * 1024) // kB -> bytes
    }

    fn read_disk() -> Result<DiskInfo, SystemError> {
        let stat = nix::sys::statvfs::statvfs("/")
            .map_err(|e| SystemError::ReadError(format!("statvfs(/): {}", e)))?;

        let block_size = stat.fragment_size() as u64;

        Ok(DiskInfo {
            disk_total_bytes: block_size * stat.blocks() as u64,
            disk_available_bytes: block_size * stat.blocks_available() as u64,
        })
    }
}