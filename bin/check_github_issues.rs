use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]));

    // correct: newlines_in_values=true (single partition)
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    let opts = CsvReadOptions::new()
        .has_header(false)
        .schema(&schema)
        .newlines_in_values(true)
        .file_extension(".csv");

    let df = ctx.read_csv("github_issues.csv", opts).await?;
    let batches = df.collect().await?;
    let correct_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // unsafe: newlines_in_values=false with low repartition threshold
    for num_partitions in [2, 4, 8, 16] {
        let config = SessionConfig::new()
            .with_target_partitions(num_partitions)
            .with_repartition_file_min_size(1024);
        let ctx = SessionContext::new_with_config(config);
        let opts = CsvReadOptions::new()
            .has_header(false)
            .schema(&schema)
            .file_extension(".csv");
        let df = ctx.read_csv("github_issues.csv", opts).await?;
        match df.collect().await {
            Ok(batches) => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let status = if rows == correct_rows { "OK" } else { "WRONG" };
                println!(
                    "unsafe {num_partitions:2}p: {rows} rows  {status}  (delta: {})",
                    rows as i64 - correct_rows as i64
                );
            }
            Err(e) => {
                println!("unsafe {num_partitions:2}p: ERROR  {e}");
            }
        }
    }

    Ok(())
}
