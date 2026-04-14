mod job;

use clap::{Parser, Subcommand};
use mesh::undergrid::{SubmitJobRequest, TaskSpec, node_agent_client::NodeAgentClient};

#[derive(Subcommand)]
enum Commands {
    /// Submit job
    Submit {
        /// Path to job TOML file
        file: String,
    },
    // // Check status of job
    // Status {
    //     /// Job ID
    //     job_id: String,
    // },

    // /// Ping a node
    // Ping,

    // /// List cluster peers
    // Peers,
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
    }

    Ok(())
}
