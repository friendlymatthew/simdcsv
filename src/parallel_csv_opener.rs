use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use datafusion::common::Result as DatafusionResult;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::{PartitionedFile, TableSchema};
use futures::StreamExt;
use object_store::{GetOptions, ObjectStore, path::Path as ObjectPath};
use tokio::sync::{Notify, OnceCell, Semaphore, oneshot};

use crate::ReaderBuilder;
use crate::partition::{PartitionClassifier, PartitionResolver, RecordAlignedRange};

type Payload = Result<
    (bytes::Bytes, tokio::sync::OwnedSemaphorePermit),
    Box<dyn std::error::Error + Send + Sync>,
>;

struct SharedState {
    resolver: Mutex<PartitionResolver>,
    partition_ready: Box<[Notify]>,
    record_aligned_ranges: Box<[OnceCell<RecordAlignedRange>]>,
    semaphore: Arc<Semaphore>,
    next_partition: AtomicUsize,
    next_allowed: AtomicUsize,
    allow_notify: Notify,
}

pub struct ParallelCsvSource {
    schema: SchemaRef,
    batch_size: usize,
    boundaries: Arc<[usize]>,
    table_schema: TableSchema,
    metrics: ExecutionPlanMetricsSet,
    state: Arc<SharedState>,
    file_path: ObjectPath,
}

impl ParallelCsvSource {
    pub fn new(
        schema: SchemaRef,
        file_path: ObjectPath,
        boundaries: Arc<[usize]>,
        batch_size: usize,
    ) -> Self {
        Self::with_concurrency(schema, file_path, boundaries, batch_size, None)
    }

    pub fn with_concurrency(
        schema: SchemaRef,
        file_path: ObjectPath,
        boundaries: Arc<[usize]>,
        batch_size: usize,
        max_concurrency: Option<usize>,
    ) -> Self {
        let table_schema = TableSchema::from_file_schema(schema.clone());
        let num_partitions = boundaries.len() - 1;
        let concurrency = max_concurrency.unwrap_or(num_partitions);

        let state = Arc::new(SharedState {
            resolver: Mutex::new(PartitionResolver::new(boundaries.clone())),
            partition_ready: (0..num_partitions).map(|_| Notify::new()).collect(),
            record_aligned_ranges: (0..num_partitions).map(|_| OnceCell::new()).collect(),
            semaphore: Arc::new(Semaphore::new(concurrency)),
            next_partition: AtomicUsize::new(0),
            next_allowed: AtomicUsize::new(0),
            allow_notify: Notify::new(),
        });

        Self {
            schema,
            batch_size,
            boundaries,
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            state,
            file_path,
        }
    }
}

impl Clone for ParallelCsvSource {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            batch_size: self.batch_size,
            boundaries: self.boundaries.clone(),
            table_schema: self.table_schema.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            state: self.state.clone(),
            file_path: self.file_path.clone(),
        }
    }
}

impl FileSource for ParallelCsvSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> DatafusionResult<Arc<dyn FileOpener>> {
        Ok(Arc::new(ParallelCsvOpener {
            schema: self.schema.clone(),
            batch_size: self.batch_size,
            boundaries: self.boundaries.clone(),
            state: self.state.clone(),
            object_store,
            file_path: self.file_path.clone(),
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut new = self.clone();
        new.batch_size = batch_size;
        Arc::new(new)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "parallel_csv"
    }

    fn supports_repartitioning(&self) -> bool {
        true
    }

    fn fmt_extra(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, ", partitions={}", self.boundaries.len() - 1)
    }
}

impl fmt::Debug for ParallelCsvSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParallelCsvSource")
            .field("partitions", &(self.boundaries.len() - 1))
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

const PADDING_BYTES: usize = 64 * 1024;

struct ParallelCsvOpener {
    schema: SchemaRef,
    batch_size: usize,
    boundaries: Arc<[usize]>,
    state: Arc<SharedState>,
    object_store: Arc<dyn ObjectStore>,
    file_path: ObjectPath,
}

impl FileOpener for ParallelCsvOpener {
    fn open(&self, _file: PartitionedFile) -> DatafusionResult<FileOpenFuture> {
        let partition_id = self.state.next_partition.fetch_add(1, Ordering::SeqCst);

        let state = self.state.clone();
        let schema = self.schema.clone();
        let batch_size = self.batch_size;
        let boundaries = self.boundaries.clone();
        let object_store = self.object_store.clone();
        let file_path = self.file_path.clone();

        let (start, end) = {
            let resolver = state.resolver.lock().unwrap();
            resolver.get_partition_range_by_id(partition_id)
        };

        let num_partitions = boundaries.len() - 1;
        let file_end = boundaries[num_partitions];
        let fetch_end = (end + PADDING_BYTES).min(file_end);
        let core_len = end - start;

        let (bytes_tx, bytes_rx) = oneshot::channel::<Payload>();

        // phase 1: fetch partitions and classify
        // this is eagerly spawned so all partitions make progress concurrently
        // permits are acquired in partition order to prevent deadlocks
        // note: the IncrementalResolver needs partition n-1 classified
        // before partition N can resolve
        let bg_state = state.clone();
        tokio::spawn(async move {
            let result: Result<_, Box<dyn std::error::Error + Send + Sync>> = async {
                loop {
                    if bg_state.next_allowed.load(Ordering::Acquire) >= partition_id {
                        break;
                    }
                    bg_state.allow_notify.notified().await;
                }
                let permit = bg_state.semaphore.clone().acquire_owned().await?;
                bg_state
                    .next_allowed
                    .fetch_max(partition_id + 1, Ordering::Release);
                bg_state.allow_notify.notify_waiters();

                let full_bytes = object_store
                    .get_opts(
                        &file_path,
                        GetOptions::new().with_range(Some(start as u64..fetch_end as u64)),
                    )
                    .await?
                    .bytes()
                    .await?;

                let classify_result = {
                    let core_bytes = full_bytes.slice(..core_len);

                    tokio::task::spawn_blocking(move || {
                        let mut scanner = PartitionClassifier::new(PADDING_BYTES);
                        scanner.ingest(&core_bytes);
                        scanner.finish()
                    })
                    .await?
                };

                // resolve our partition and neighboring partitions (partition_id and onwards)
                {
                    let mut resolver = bg_state.resolver.lock().unwrap();
                    for (partition_id, split) in resolver.submit(partition_id, classify_result) {
                        let _ = bg_state.record_aligned_ranges[partition_id].set(split);
                        bg_state.partition_ready[partition_id].notify_one();
                    }
                }

                Ok((full_bytes, permit))
            }
            .await;

            match result {
                Ok(payload) => {
                    let _ = bytes_tx.send(Ok(payload));
                }
                Err(e) => {
                    let _ = bytes_tx.send(Err(e));
                }
            }
        });

        // phase 2: wait for resolved ranges, then decode
        // the resolved split gives us record-aligned byte boundaries
        Ok(Box::pin(async move {
            loop {
                if state.record_aligned_ranges[partition_id].get().is_some() {
                    break;
                }

                state.partition_ready[partition_id].notified().await;
            }

            let split = state.record_aligned_ranges[partition_id].get().unwrap();

            let (full_bytes, _permit) = bytes_rx
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .map_err(DataFusionError::External)?;

            let record_aligned_bytes = full_bytes.slice((split.start - start)..(split.end - start));

            let batches = tokio::task::spawn_blocking(move || {
                let reader = ReaderBuilder::new(schema)
                    .with_batch_size(batch_size)
                    .build_buffered(&record_aligned_bytes[..]);

                reader.collect::<Result<Vec<_>, _>>()
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .map_err(DataFusionError::from)?;

            Ok(futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)).boxed())
        }))
    }
}
