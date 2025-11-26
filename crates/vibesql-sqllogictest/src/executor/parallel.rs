//! Parallel test execution logic.

use futures::{stream, Future, FutureExt, StreamExt};

use crate::{Connections, MakeConnection};
use super::core::{AsyncDB, Runner, RunnerLocals};

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Accept the tasks, spawn jobs task to run slt test. the tasks are (AsyncDB, slt filename)
    /// pairs.
    // TODO: This is not a good interface, as the `make_conn` passed to `new` is unused but we
    // accept a new `conn_builder` here. May change `MakeConnection` to support specifying the
    // database name in the future.
    pub async fn run_parallel_async<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), crate::error_handling::ParallelTestError>
    where
        Fut: Future<Output = D>,
    {
        let files = glob::glob(glob).expect("failed to read glob pattern");
        let mut tasks = vec![];

        for (idx, file) in files.enumerate() {
            // for every slt file, we create a database against table conflict
            let file = file.unwrap();
            let filename = file.to_str().expect("not a UTF-8 filename");

            // Skip files that don't match the partitioner.
            if !self.partitioner.matches(filename) {
                continue;
            }

            let db_name = filename.replace([' ', '.', '-', '/'], "_");

            self.conn
                .run_default(&format!("CREATE DATABASE {db_name};"))
                .await
                .expect("create db failed");
            let target = hosts[idx % hosts.len()].clone();

            let mut locals = RunnerLocals::default();
            locals.set_var("__DATABASE__".to_owned(), db_name.clone());

            let mut tester = Runner {
                conn: Connections::new(move || {
                    conn_builder(target.clone(), db_name.clone()).map(Ok)
                }),
                validator: self.validator,
                normalizer: self.normalizer,
                column_type_validator: self.column_type_validator,
                partitioner: self.partitioner.clone(),
                substitution_on: self.substitution_on,
                sort_mode: self.sort_mode,
                result_mode: self.result_mode,
                hash_threshold: self.hash_threshold,
                labels: self.labels.clone(),
                locals,
                auto_switch_dialect: self.auto_switch_dialect,
                current_dialect: self.current_dialect.clone(),
            };

            tasks.push(async move {
                let filename = file.to_string_lossy().to_string();
                tester.run_file_async(filename).await
            })
        }

        let tasks = stream::iter(tasks).buffer_unordered(jobs);
        let errors: Vec<_> = tasks
            .filter_map(|result| async { result.err() })
            .collect()
            .await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(crate::error_handling::ParallelTestError { errors })
        }
    }

    /// sync version of `run_parallel_async`
    pub fn run_parallel<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), crate::error_handling::ParallelTestError>
    where
        Fut: Future<Output = D>,
    {
        futures::executor::block_on(self.run_parallel_async(glob, hosts, conn_builder, jobs))
    }
}
