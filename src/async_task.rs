use std::{
    future::Future,
    thread,
    time::{Duration, Instant},
};

use bevy::{
    prelude::Component,
    tasks::{AsyncComputeTaskPool, Task, futures::check_ready},
};

/// Generic component wrapping a background [`Task`] spawned on Bevy's [`AsyncComputeTaskPool`].
///
/// Shared by both the headless server and the GUI client so async work runs off the main tick
/// loop and is polled non-blockingly in regular Bevy `Update` systems.
#[derive(Component)]
pub struct AsyncTask<T: Send + 'static>(Task<T>);

impl<T: Send + 'static> AsyncTask<T> {
    /// Spawns `future` onto [`AsyncComputeTaskPool`] and wraps the handle in a Bevy component.
    ///
    /// # Arguments
    /// * `future` - Async computation returning `T` that runs on a background worker thread.
    ///
    /// # Example
    /// ```no_run
    /// use bevy_server_in_docker_poc::async_task::AsyncTask;
    ///
    /// let _task = AsyncTask::spawn(async { 2 + 2 });
    /// ```
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        let task = AsyncComputeTaskPool::get().spawn(future);
        Self(task)
    }

    /// Polls the underlying task once without blocking and returns `Some(T)` when complete.
    ///
    /// Callers should despawn or remove the component once `Some(T)` is returned.
    ///
    /// # Example
    /// ```no_run
    /// use bevy_server_in_docker_poc::async_task::AsyncTask;
    ///
    /// fn poll(task: &mut AsyncTask<u32>) -> Option<u32> {
    ///     task.try_take()
    /// }
    /// ```
    pub fn try_take(&mut self) -> Option<T> {
        check_ready(&mut self.0)
    }
}

/// Output produced by [`simulate_async_work`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncWorkOutput {
    pub elapsed_ms: u64,
    pub summary: String,
}

/// Executes a single async work step on the background compute pool.
///
/// # Arguments
/// * `step_delay` - Duration spent simulating compute/I/O work on the worker thread.
async fn run_work_step(step_delay: Duration) {
    if !step_delay.is_zero() {
        thread::sleep(step_delay);
    }
}

/// Runs a two-step async background job and returns the elapsed duration and summary text.
///
/// Intended to run inside [`AsyncTask::spawn`] so the main Bevy update loop remains responsive.
///
/// # Arguments
/// * `label` - Human-readable identifier included in the returned summary.
/// * `delay` - Total simulated work duration split across two `.await` steps.
///
/// # Example
/// ```
/// use std::time::Duration;
/// use bevy::tasks::block_on;
/// use bevy_server_in_docker_poc::async_task::simulate_async_work;
///
/// let output = block_on(simulate_async_work("demo", Duration::from_millis(1)));
/// assert!(output.summary.contains("demo"));
/// ```
pub async fn simulate_async_work(label: &str, delay: Duration) -> AsyncWorkOutput {
    let started = Instant::now();
    let half = delay / 2;

    run_work_step(half).await;
    run_work_step(delay.saturating_sub(half)).await;

    let elapsed_ms = started.elapsed().as_millis() as u64;
    AsyncWorkOutput {
        elapsed_ms,
        summary: format!("{label} completed"),
    }
}

#[cfg(test)]
#[path = "async_task_test.rs"]
mod tests;
