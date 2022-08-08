use azure_kusto_data::prelude::KustoClient;
use chrono::{DateTime, Duration, FixedOffset, SecondsFormat};
use color_eyre::Result;
use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use itertools::Itertools;
use regex::Regex;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoffBuilder, RetryTransientMiddleware};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{select, sync::Mutex, time};

const KUSTO_CONN_STRING: &str = "server=https://engsrvprod.kusto.windows.net";
const KUSTO_DATABASE: &str = "engineeringdata";
const HELIX_LOGS_QUERY: &str = r#"TimelineRecords
| where FinishTime > ago(7d)
| where Result != "skipped"
| where LogUri != ""
| where WorkerName startswith "NetCore1ESPool"
| where Name has "helix"
| join kind=inner (TimelineBuilds | project Definition, BuildId, Repository) on BuildId
"#;

#[derive(Debug)]
struct OutputRecord {
    wasted_time: i64,
    start_time: DateTime<FixedOffset>,
    worker_name: String,
    definition_name: String,
    repo: String,
}
struct Config {
    client: ClientWithMiddleware,
    work_done: AtomicUsize,
    output: Mutex<Vec<OutputRecord>>,
    send_job_regex: Regex,
    job_finished_regex: Regex,
    pat: &'static str,
}

fn create_retry_client() -> ClientWithMiddleware {
    let retry_policy = ExponentialBackoffBuilder::default()
        .build_with_total_retry_duration(time::Duration::from_secs(10));
    ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

async fn worker_task(config: Arc<Config>, dt: Vec<serde_json::Value>) {
    // Get the LogUri
    let log_uri = dt[18].as_str().unwrap();

    if let Some(wait_time) = get_helix_wait_time(log_uri, &config).await {
        let record = OutputRecord {
            wasted_time: wait_time.num_seconds(),
            start_time: DateTime::parse_from_rfc3339(dt[6].as_str().unwrap()).unwrap(),
            worker_name: dt[12].as_str().unwrap().to_string(),
            definition_name: dt[25].as_str().unwrap().to_string(),
            repo: dt[27].as_str().unwrap().to_string(),
        };

        let mut output = config.output.lock().await;
        output.push(record);
    }

    config.work_done.fetch_add(1, Ordering::Relaxed);
}

async fn get_helix_wait_time(log_uri: &str, config: &Arc<Config>) -> Option<Duration> {
    let log_text = config
        .client
        .get(log_uri)
        .basic_auth("", Some(config.pat))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;

    let mut first_start = None;
    let mut last_end = None;

    for line in log_text.lines() {
        if first_start == None {
            if let Some((dt, _)) = find_helix_job_info(&config.send_job_regex, line) {
                first_start = Some(dt);
            }
        }

        if let Some((dt, _)) = find_helix_job_info(&config.job_finished_regex, line) {
            last_end = Some(dt);
        }
    }

    if let (Some(first_start), Some(last_end)) = (first_start, last_end) {
        Some(last_end - first_start)
    } else {
        None
    }
}

fn find_helix_job_info<'a>(
    regex: &Regex,
    line: &'a str,
) -> Option<(DateTime<FixedOffset>, &'a str)> {
    let caps = regex.captures(line)?;

    if caps.len() != 3 {
        return None;
    }

    let dt = DateTime::parse_from_rfc3339(caps.get(1).unwrap().as_str()).ok()?;
    let job_id = caps.get(2).unwrap().as_str();

    Some((dt, job_id))
}

#[tokio::main]
async fn main() -> Result<()> {
    let kusto = KustoClient::try_from(KUSTO_CONN_STRING.to_string())?;

    let mut task = kusto
        .execute_query(KUSTO_DATABASE, HELIX_LOGS_QUERY)
        .into_future();

    let pb = ProgressBar::new_spinner();
    pb.set_message("Fetching helix logs");

    let result;
    loop {
        pb.tick();
        select! {
            res = &mut task => {
                 result = res?;
                 break
            },
            _ = time::sleep(time::Duration::from_millis(100)) => {},
        }
    }

    pb.finish_with_message("Done fetching helix logs!");

    let config = Arc::new(Config {
        client: create_retry_client(),
        work_done: AtomicUsize::new(0),
        output: Mutex::new(Vec::with_capacity(100000)),
        send_job_regex: Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.[0-9]+Z)   Waiting for completion of job ([0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})").unwrap(),
        job_finished_regex: Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.[0-9]+Z)   Job ([0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})").unwrap(),
        pat: Box::leak(Box::new(std::env::args().nth(1).unwrap())),
    });

    let mut items = 0usize;
    for r in result.primary_results() {
        for row in r.rows {
            let config = config.clone();
            tokio::spawn(async move {
                worker_task(config, row).await;
            });
            items += 1;
        }
    }

    let pb = ProgressBar::new(items as u64);
    pb.set_message("Processing helix logs");
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {eta} {msg}",
        )
        .unwrap(),
    );

    while items != config.work_done.load(Ordering::Relaxed) {
        time::sleep(time::Duration::from_millis(100)).await;
        pb.set_position(config.work_done.load(Ordering::Relaxed) as u64)
    }

    pb.finish_with_message("Done parsing helix logs :)");

    let lock = config.output.lock().await;
    export_csv(&lock, "helix_logs.csv");

    println!("good bye");

    Ok(())
}

fn export_csv(data: &[OutputRecord], filename: &str) {
    let mut wtr = csv::Writer::from_path(filename).unwrap();
    wtr.write_record(&[
        "start_time",
        "worker_name",
        "wasted_time",
        "definition",
        "repository",
    ])
    .unwrap();
    for record in data.iter().progress().with_message("writing to csv") {
        wtr.write_record(&[
            &record.start_time.to_rfc3339_opts(SecondsFormat::Secs, true),
            &record.worker_name,
            &record.wasted_time.to_string(),
            &record.definition_name,
            &record.repo,
        ])
        .unwrap();
    }
}
