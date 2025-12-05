//! Scheduler manager for coordinating schedule execution
//!
//! Manages the lifecycle of scheduled tasks and cron jobs, including:
//! - Periodic polling for due tasks
//! - Triggering execution
//! - Updating status and history

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::info;

use super::executor::{ScheduleExecutor, ScheduleExecutorConfig};

/// Configuration for scheduler manager
#[derive(Debug, Clone)]
pub struct SchedulerManagerConfig {
    /// Poll interval for checking due schedules
    pub poll_interval_secs: u64,
    /// Schedule executor configuration
    pub executor_config: ScheduleExecutorConfig,
    /// Enable the scheduler
    pub enabled: bool,
}

impl Default for SchedulerManagerConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 10,
            executor_config: ScheduleExecutorConfig::default(),
            enabled: true,
        }
    }
}

/// Manages scheduled function execution
pub struct SchedulerManager {
    config: SchedulerManagerConfig,
    #[allow(dead_code)]
    executor: Arc<ScheduleExecutor>,
    task_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl SchedulerManager {
    pub fn new(config: SchedulerManagerConfig) -> Self {
        let executor = Arc::new(ScheduleExecutor::new(config.executor_config.clone()));

        Self { config, executor, task_handle: Arc::new(RwLock::new(None)) }
    }

    /// Start the scheduler background task
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            info!("Scheduler is disabled");
            return Ok(());
        }

        info!("Starting scheduler with {} second poll interval", self.config.poll_interval_secs);

        let poll_interval_secs = self.config.poll_interval_secs;

        let task = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(poll_interval_secs));

            loop {
                interval.tick().await;

                // Poll for due schedules
                // This will be connected to the actual database when we have
                // the storage layer integrated

                info!("Scheduler poll tick");
            }
        });

        *self.task_handle.write().await = Some(task);
        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&self) {
        if let Some(handle) = self.task_handle.write().await.take() {
            handle.abort();
            info!("Scheduler stopped");
        }
    }

    /// Check if scheduler is running
    pub async fn is_running(&self) -> bool {
        self.task_handle.read().await.is_some()
    }
}

impl Drop for SchedulerManager {
    fn drop(&mut self) {
        // Cleanup is async, but Drop is sync
        // In a real implementation, we'd need to handle this more carefully
        // For now, we just log that the manager is being dropped
        info!("SchedulerManager dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = SchedulerManagerConfig::default();
        assert_eq!(config.poll_interval_secs, 10);
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let manager = SchedulerManager::new(SchedulerManagerConfig::default());
        assert!(!manager.is_running().await);
    }

    #[tokio::test]
    async fn test_manager_start_stop() {
        let manager = SchedulerManager::new(SchedulerManagerConfig::default());

        manager.start().await.expect("Failed to start scheduler");
        assert!(manager.is_running().await);

        manager.stop().await;
        assert!(!manager.is_running().await);
    }

    #[tokio::test]
    async fn test_disabled_scheduler() {
        let config = SchedulerManagerConfig { enabled: false, ..Default::default() };

        let manager = SchedulerManager::new(config);
        manager.start().await.expect("Should not fail when disabled");
        assert!(!manager.is_running().await);
    }
}
