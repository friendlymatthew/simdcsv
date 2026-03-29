use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
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
use tokio::sync::{Barrier, OnceCell};

use crate::ReaderBuilder;
use crate::monoid::ClassifyResult;
use crate::partition::{PartitionResolver, ResolvedSplit, SubPartition};

pub trait CsvDecoder: Send + Sync + 'static {
    fn decode(
        &self,
        schema: SchemaRef,
        batch_size: usize,
        data: &[u8],
    ) -> Result<Vec<RecordBatch>, ArrowError>;
}

#[derive(Clone)]
pub struct ArrowCsvDecoder;

impl CsvDecoder for ArrowCsvDecoder {
    fn decode(
        &self,
        schema: SchemaRef,
        batch_size: usize,
        data: &[u8],
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        let reader = arrow_csv::ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .build_buffered(data)?;

        reader.collect()
    }
}

#[derive(Clone)]
pub struct ArrowCsv2Decoder;

impl CsvDecoder for ArrowCsv2Decoder {
    fn decode(
        &self,
        schema: SchemaRef,
        batch_size: usize,
        data: &[u8],
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        let reader = ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .build_buffered(data);

        reader.collect()
    }
}

struct SharedState {
    coordinator: Mutex<PartitionResolver>,
    barrier: Barrier,
    resolved: OnceCell<Vec<ResolvedSplit>>,
    next_partition: AtomicUsize,
    classify_results: Mutex<Vec<Option<ClassifyResult>>>,
}

pub struct ParallelCsvSource<D: CsvDecoder> {
    schema: SchemaRef,
    batch_size: usize,
    boundaries: Arc<[usize]>,
    table_schema: TableSchema,
    metrics: ExecutionPlanMetricsSet,
    state: Arc<SharedState>,
    decoder: D,
    file_path: ObjectPath,
}

impl<D: CsvDecoder + Clone> ParallelCsvSource<D> {
    pub fn new(
        schema: SchemaRef,
        file_path: ObjectPath,
        boundaries: Arc<[usize]>,
        batch_size: usize,
        decoder: D,
    ) -> Self {
        let table_schema = TableSchema::from_file_schema(schema.clone());
        let num_partitions = boundaries.len() - 1;

        let state = Arc::new(SharedState {
            coordinator: Mutex::new(PartitionResolver::new(boundaries.clone())),
            barrier: Barrier::new(num_partitions),
            resolved: OnceCell::new(),
            next_partition: AtomicUsize::new(0),
            classify_results: Mutex::new((0..num_partitions).map(|_| None).collect()),
        });

        Self {
            schema,
            batch_size,
            boundaries,
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            state,
            decoder,
            file_path,
        }
    }
}

impl<D: CsvDecoder + Clone> Clone for ParallelCsvSource<D> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            batch_size: self.batch_size,
            boundaries: self.boundaries.clone(),
            table_schema: self.table_schema.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            state: self.state.clone(),
            decoder: self.decoder.clone(),
            file_path: self.file_path.clone(),
        }
    }
}

impl<D: CsvDecoder + Clone> FileSource for ParallelCsvSource<D> {
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
            decoder: Arc::new(self.decoder.clone()),
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

impl<D: CsvDecoder + Clone> fmt::Debug for ParallelCsvSource<D> {
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
    decoder: Arc<dyn CsvDecoder>,
    object_store: Arc<dyn ObjectStore>,
    file_path: ObjectPath,
}

impl FileOpener for ParallelCsvOpener {
    fn open(&self, _file: PartitionedFile) -> DatafusionResult<FileOpenFuture> {
        let partition_idx = self.state.next_partition.fetch_add(1, Ordering::SeqCst);

        let state = self.state.clone();
        let schema = self.schema.clone();
        let batch_size = self.batch_size;
        let boundaries = self.boundaries.clone();
        let decoder = self.decoder.clone();
        let object_store = self.object_store.clone();
        let file_path = self.file_path.clone();

        let (start, end) = {
            let coord = state.coordinator.lock().unwrap();
            coord.partition_range(partition_idx)
        };

        let num_partitions = boundaries.len() - 1;
        let file_end = boundaries[num_partitions];

        let fetch_start = start.saturating_sub(PADDING_BYTES);
        let fetch_end = (end + PADDING_BYTES).min(file_end);

        Ok(Box::pin(async move {
            // phase 1: fetch a range with some padding on both sides
            let result = object_store
                .get_opts(
                    &file_path,
                    GetOptions::new().with_range(Some(fetch_start as u64..fetch_end as u64)),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let full_bytes = result
                .bytes()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // phase 2: classify only the core [start, end) range
            let core_offset = start - fetch_start;
            let core_len = end - start;
            let classify_result = {
                let core_bytes = full_bytes.slice(core_offset..core_offset + core_len);
                tokio::task::spawn_blocking(move || {
                    let mut scanner = SubPartition::new(PADDING_BYTES);
                    scanner.ingest(&core_bytes);
                    scanner.finish()
                })
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            {
                let mut results = state.classify_results.lock().unwrap();
                results[partition_idx] = Some(classify_result);
            }

            state.barrier.wait().await;

            // phase 3: resolve quotes and produce aligned ranges
            let resolved = state
                .resolved
                .get_or_init(|| async {
                    let mut resolver = state.coordinator.lock().unwrap();
                    {
                        let mut results = state.classify_results.lock().unwrap();
                        for (i, result) in results.iter_mut().enumerate() {
                            resolver.submit(i, result.take().unwrap());
                        }
                    }
                    resolver.resolve()
                })
                .await;

            // phase 4: slice to get the aligned range
            let split = &resolved[partition_idx];
            let local_start = split
                .start
                .checked_sub(fetch_start)
                .expect("resolved start before fetch_start");
            let local_end = split.end - fetch_start;

            let aligned_bytes = full_bytes.slice(local_start..local_end);

            let batches = tokio::task::spawn_blocking(move || {
                decoder.decode(schema, batch_size, &aligned_bytes)
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .map_err(DataFusionError::from)?;

            let stream = futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>));

            Ok(stream.boxed())
        }))
    }
}
