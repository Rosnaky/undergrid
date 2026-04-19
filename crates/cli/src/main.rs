mod job;

use clap::{Parser, Subcommand};
use mesh::undergrid::{
    GetJobStatusRequest, JobStatus as ProtoJobStatus, SubmitJobRequest, TaskSpec,
    node_agent_client::NodeAgentClient,
};

#[derive(Subcommand)]
enum Commands {
    /// Submit job
    Submit {
        /// Path to job TOML file
        file: String,
    },
    // Check status of job
    Status {
        /// Job ID
        job_id: String,
    },
}

#[derive(Parser)]
#[command(name = "undergrid", about = "Undergrid CLI")]
struct Cli {
    #[arg(short, long)]
    node: String,

    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Submit { file } => {
            let contents = std::fs::read_to_string(&file)
                .map_err(|e| format!("Failed to read '{}': {}", file, e))?;
            let job_file: job::JobFile =
                toml::from_str(&contents).map_err(|e| format!("Invalid TOML: {}", e))?;

            let tasks: Vec<TaskSpec> = job_file
                .tasks
                .iter()
                .map(|(id, def)| def.to_proto(id))
                .collect();

            println!(
                "Submitting job '{}' with {} tasks...",
                job_file.job.id,
                tasks.len()
            );

            let mut client = NodeAgentClient::connect(cli.node).await?;
            let response = client
                .submit_job(SubmitJobRequest {
                    job_id: job_file.job.id.clone(),
                    tasks,
                })
                .await?
                .into_inner();

            if response.accepted {
                println!("Job '{}' submitted successfully", job_file.job.id);
            } else {
                eprintln!("Job rejected: {}", response.error);
                std::process::exit(1);
            }
        }
        Commands::Status { job_id } => {
            let mut client = NodeAgentClient::connect(cli.node).await?;
            let response = client
                .get_job_status(GetJobStatusRequest {
                    job_id: job_id.clone(),
                })
                .await?
                .into_inner();

            let status =
                ProtoJobStatus::try_from(response.status).unwrap_or(ProtoJobStatus::Pending);

            if response.found {
                match status {
                    ProtoJobStatus::Pending => println!("Job '{}': Pending", job_id),
                    ProtoJobStatus::Running => {
                        let secs = response.elapsed_ms / 1000;
                        println!("Job '{}': Running ({secs}s elapsed)", job_id);
                    }
                    ProtoJobStatus::Completed => {
                        let secs = response.elapsed_ms / 1000;
                        println!("Job '{}': Completed in {secs}s", job_id);
                    }
                    ProtoJobStatus::Failed => {
                        let secs = response.elapsed_ms / 1000;
                        println!(
                            "Job '{}': Failed after {secs}s — {}",
                            job_id, response.error
                        );
                    }
                }
            } else {
                println!("Job '{}' not found", job_id);
            }
        }
    }

    Ok(())
}
