use azure_kusto_data::prelude::KustoClient;
use chrono::{DateTime, Duration, FixedOffset};
use color_eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoffBuilder, RetryTransientMiddleware};
use reqwest_retry_after::RetryAfterMiddleware;
use serde::Serialize;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    select,
    sync::mpsc::{self, UnboundedSender},
    time,
};

const KUSTO_CONN_STRING: &str = "server=https://1es.kusto.windows.net;AAD Federated Security=True";
const KUSTO_DATABASE: &str = "AzureDevOps";
const HELIX_LOGS_QUERY: &str = r#"BuildTimelineRecord
| where StartTime > ago(7d)
| where OrganizationName == "dnceng"
| where WorkerName startswith "NetCore1ESPool"
| where Type == "Task"
| where isnotempty(LogId)
| where RecordName has "Send" and RecordName has "Helix"
| join kind = inner (Build
    | project BuildStartTime = StartTime, BuildId, BuildEndTime = FinishTime, ProjectId)
    on BuildId, ProjectId
| project
    OrganizationName,
    ProjectId,
    RepositoryId,
    BuildId,
    LogId,
    HelixStart = StartTime,
    BuildStartTime,
    BuildEndTime
"#;

#[derive(Debug, Serialize)]
struct OutputRecord {
    repo_id: String,
    build_start: String,
    helix_test_start: String,
    build_end: String,
    wasted_time_sec: i64,
}

struct Config {
    client: ClientWithMiddleware,
    work_done: AtomicUsize,
    send_job_regex: Regex,
    job_finished_regex: Regex,
    pat: &'static str,
}

fn create_retry_client() -> ClientWithMiddleware {
    let retry_policy = ExponentialBackoffBuilder::default()
        .build_with_total_retry_duration(time::Duration::from_secs(10));
    ClientBuilder::new(
        reqwest::ClientBuilder::new()
            .user_agent("helix-spinster")
            .build()
            .unwrap(),
    )
    //.with(RetryAfterMiddleware::new())
    .with(RetryTransientMiddleware::new_with_policy(retry_policy))
    .build()
}

async fn worker_task(
    config: Arc<Config>,
    dt: Vec<serde_json::Value>,
    tx: UnboundedSender<OutputRecord>,
) {
    // Get the LogUri
    let mut log_uri = String::with_capacity(128);
    log_uri.push_str("https://dev.azure.com/");
    log_uri.push_str(dt[0].as_str().unwrap());
    log_uri.push('/');
    log_uri.push_str(dt[1].as_str().unwrap());
    log_uri.push_str("/_apis/build/builds/");
    log_uri.push_str(&dt[3].as_u64().unwrap().to_string());
    log_uri.push_str("/logs/");
    log_uri.push_str(&dt[4].as_u64().unwrap().to_string());

    if let Some(wait_time) = get_helix_wait_time(&log_uri, &config).await {
        let record = OutputRecord {
            repo_id: dt[2].as_str().unwrap().to_string(),
            build_start: dt[6].as_str().unwrap().to_string(),
            helix_test_start: dt[5].as_str().unwrap().to_string(),
            build_end: dt[7].as_str().unwrap().to_string(),
            wasted_time_sec: wait_time.num_seconds(),
        };

        tx.send(record).unwrap();
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
        //if first_start == None {
        if let Some((dt, _)) = find_helix_job_info(&config.send_job_regex, line) {
            first_start = Some(dt);
        }
        //}

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
        send_job_regex: Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.[0-9]+Z)   Waiting for completion of job ([0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})").unwrap(),
        job_finished_regex: Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.[0-9]+Z)   Job ([0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})").unwrap(),
        pat: Box::leak(Box::new(std::env::args().nth(1).unwrap())),
    });

    let mut items = 0;
    let (tx, mut rx) = mpsc::unbounded_channel();

    for r in result.primary_results() {
        for row in r.rows {
            let config = config.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                worker_task(config, row, tx).await;
            });
            items += 1;
        }
    }

    let pb = ProgressBar::new(items);
    pb.set_message("Processing helix logs");
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {eta} {msg}",
        )
        .unwrap(),
    );

    let mut writer = csv::Writer::from_path(Path::new("helix_logs.csv")).unwrap();
    while let Some(record) = rx.recv().await {
        pb.inc(1);
        writer.serialize(record).unwrap();
    }

    pb.finish_with_message("Done parsing helix logs :)");

    Ok(())
}
