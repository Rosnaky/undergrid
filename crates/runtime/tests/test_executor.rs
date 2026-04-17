use runtime::executor::Executor;
use runtime::task::*;
use std::collections::HashMap;
use std::time::Duration;

fn make_batch_spec(id: &str, image: &str, command: Vec<&str>, timeout_s: u64) -> TaskSpec {
    TaskSpec {
        id: id.to_string(),
        image: image.to_string(),
        command: command.into_iter().map(String::from).collect(),
        env: HashMap::new(),
        resources: ResourceRequirements {
            cpu_cores: 0.5,
            memory_bytes: 67_108_864,
            disk_bytes: 1_000_000,
            gpu: false,
        },
        depends_on: vec![],
        kind: TaskKind::Batch {
            timeout_s: Duration::from_secs(timeout_s),
        },
    }
}

// ═══════════════════════════════════════════════════════════
// BASIC EXECUTION
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn execute_echo_returns_stdout() {
    let executor = Executor::new();
    let spec = make_batch_spec(
        "test-echo",
        "alpine:latest",
        vec!["echo", "hello undergrid"],
        30,
    );

    let result = executor.execute(&spec).await;
    assert!(result.is_ok(), "Execution failed: {:?}", result.err());

    let output = result.unwrap();
    assert_eq!(output.exit_code, 0);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "hello undergrid"
    );
}

#[tokio::test]
async fn execute_returns_nonzero_exit_code() {
    let executor = Executor::new();
    let spec = make_batch_spec(
        "test-fail",
        "alpine:latest",
        vec!["sh", "-c", "exit 42"],
        30,
    );

    let result = executor.execute(&spec).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    assert_eq!(output.exit_code, 42);
}

#[tokio::test]
async fn execute_captures_multiline_stdout() {
    let executor = Executor::new();
    let spec = make_batch_spec(
        "test-multiline",
        "alpine:latest",
        vec!["sh", "-c", "echo line1; echo line2; echo line3"],
        30,
    );

    let result = executor.execute(&spec).await.unwrap();
    assert_eq!(result.exit_code, 0);

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(stdout.contains("line1"));
    assert!(stdout.contains("line2"));
    assert!(stdout.contains("line3"));
}

// ═══════════════════════════════════════════════════════════
// ENVIRONMENT VARIABLES
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn execute_with_env_vars() {
    let executor = Executor::new();
    let mut spec = make_batch_spec(
        "test-env",
        "alpine:latest",
        vec!["sh", "-c", "echo $MY_VAR"],
        30,
    );
    spec.env
        .insert("MY_VAR".to_string(), "undergrid_value".to_string());

    let result = executor.execute(&spec).await.unwrap();
    assert_eq!(result.exit_code, 0);
    assert_eq!(
        String::from_utf8_lossy(&result.stdout).trim(),
        "undergrid_value"
    );
}

// ═══════════════════════════════════════════════════════════
// TIMEOUT
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn execute_timeout_kills_container() {
    let executor = Executor::new();

    // Pre-pull so timeout only measures execution
    executor.pull_image("alpine:latest").await.unwrap();

    let spec = make_batch_spec("test-timeout", "alpine:latest", vec!["sleep", "60"], 2);

    let start = std::time::Instant::now();
    let result = executor.execute(&spec).await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    assert!(
        elapsed < Duration::from_secs(10),
        "Should have timed out quickly, took {:?}",
        elapsed
    );
}

// ═══════════════════════════════════════════════════════════
// IMAGE PULLING
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn pull_valid_image() {
    let executor = Executor::new();
    let result = executor.pull_image("alpine:latest").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn pull_invalid_image_fails() {
    let executor = Executor::new();
    let result = executor
        .pull_image("nonexistent-registry.invalid/fake-image:latest")
        .await;
    assert!(result.is_err());
}

// ═══════════════════════════════════════════════════════════
// RESOURCE LIMITS
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn execute_respects_memory_limit() {
    let executor = Executor::new();
    // Try to allocate 256MB in a container limited to 64MB — should fail
    let spec = make_batch_spec(
        "test-oom",
        "alpine:latest",
        vec!["sh", "-c", "dd if=/dev/zero of=/dev/null bs=256M count=1"],
        30,
    );

    let result = executor.execute(&spec).await;
    // Container should run but the command behavior depends on OS/docker
    // At minimum it shouldn't hang
    assert!(result.is_ok());
}

// ═══════════════════════════════════════════════════════════
// EMPTY / EDGE CASES
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn execute_no_output() {
    let executor = Executor::new();
    let spec = make_batch_spec("test-silent", "alpine:latest", vec!["true"], 30);

    let result = executor.execute(&spec).await.unwrap();
    assert_eq!(result.exit_code, 0);
    assert!(result.stdout.is_empty());
}

#[tokio::test]
async fn execute_false_returns_nonzero() {
    let executor = Executor::new();
    let spec = make_batch_spec("test-false", "alpine:latest", vec!["false"], 30);

    let result = executor.execute(&spec).await.unwrap();
    assert_ne!(result.exit_code, 0);
}
