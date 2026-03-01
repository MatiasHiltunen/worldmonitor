use std::{
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use chrono::Utc;
use futures::future::join_all;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const INFRA_CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListServiceStatusesRequest {
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListServiceStatusesResponse {
    pub statuses: Vec<ServiceStatus>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    pub id: String,
    pub name: String,
    pub status: String,
    pub description: String,
    pub url: String,
    pub checked_at: i64,
    pub latency_ms: i64,
}

#[derive(Clone)]
struct CacheEntry {
    statuses: Vec<ServiceStatus>,
    expires_at: Instant,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParserKind {
    Default,
    Aws,
    Rss,
    Gcp,
    Instatus,
    Statusio,
    Slack,
    Stripe,
    Incidentio,
}

#[derive(Clone, Copy)]
struct ServiceDef {
    id: &'static str,
    name: &'static str,
    status_page: &'static str,
    parser: ParserKind,
}

const SERVICES: [ServiceDef; 30] = [
    ServiceDef {
        id: "aws",
        name: "AWS",
        status_page: "https://health.aws.amazon.com/health/status",
        parser: ParserKind::Aws,
    },
    ServiceDef {
        id: "azure",
        name: "Azure",
        status_page: "https://azure.status.microsoft/en-us/status/feed/",
        parser: ParserKind::Rss,
    },
    ServiceDef {
        id: "gcp",
        name: "Google Cloud",
        status_page: "https://status.cloud.google.com/incidents.json",
        parser: ParserKind::Gcp,
    },
    ServiceDef {
        id: "cloudflare",
        name: "Cloudflare",
        status_page: "https://www.cloudflarestatus.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "vercel",
        name: "Vercel",
        status_page: "https://www.vercel-status.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "netlify",
        name: "Netlify",
        status_page: "https://www.netlifystatus.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "digitalocean",
        name: "DigitalOcean",
        status_page: "https://status.digitalocean.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "render",
        name: "Render",
        status_page: "https://status.render.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "railway",
        name: "Railway",
        status_page: "https://railway.instatus.com/summary.json",
        parser: ParserKind::Instatus,
    },
    ServiceDef {
        id: "github",
        name: "GitHub",
        status_page: "https://www.githubstatus.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "gitlab",
        name: "GitLab",
        status_page: "https://status.gitlab.com/1.0/status/5b36dc6502d06804c08349f7",
        parser: ParserKind::Statusio,
    },
    ServiceDef {
        id: "npm",
        name: "npm",
        status_page: "https://status.npmjs.org/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "docker",
        name: "Docker Hub",
        status_page: "https://www.dockerstatus.com/1.0/status/533c6539221ae15e3f000031",
        parser: ParserKind::Statusio,
    },
    ServiceDef {
        id: "bitbucket",
        name: "Bitbucket",
        status_page: "https://bitbucket.status.atlassian.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "circleci",
        name: "CircleCI",
        status_page: "https://status.circleci.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "jira",
        name: "Jira",
        status_page: "https://jira-software.status.atlassian.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "confluence",
        name: "Confluence",
        status_page: "https://confluence.status.atlassian.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "linear",
        name: "Linear",
        status_page: "https://linearstatus.com/api/v2/status.json",
        parser: ParserKind::Incidentio,
    },
    ServiceDef {
        id: "slack",
        name: "Slack",
        status_page: "https://slack-status.com/api/v2.0.0/current",
        parser: ParserKind::Slack,
    },
    ServiceDef {
        id: "discord",
        name: "Discord",
        status_page: "https://discordstatus.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "zoom",
        name: "Zoom",
        status_page: "https://www.zoomstatus.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "notion",
        name: "Notion",
        status_page: "https://www.notion-status.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "openai",
        name: "OpenAI",
        status_page: "https://status.openai.com/api/v2/status.json",
        parser: ParserKind::Incidentio,
    },
    ServiceDef {
        id: "anthropic",
        name: "Anthropic",
        status_page: "https://status.claude.com/api/v2/status.json",
        parser: ParserKind::Incidentio,
    },
    ServiceDef {
        id: "replicate",
        name: "Replicate",
        status_page: "https://www.replicatestatus.com/api/v2/status.json",
        parser: ParserKind::Incidentio,
    },
    ServiceDef {
        id: "stripe",
        name: "Stripe",
        status_page: "https://status.stripe.com/current",
        parser: ParserKind::Stripe,
    },
    ServiceDef {
        id: "twilio",
        name: "Twilio",
        status_page: "https://status.twilio.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "datadog",
        name: "Datadog",
        status_page: "https://status.datadoghq.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "sentry",
        name: "Sentry",
        status_page: "https://status.sentry.io/api/v2/status.json",
        parser: ParserKind::Default,
    },
    ServiceDef {
        id: "supabase",
        name: "Supabase",
        status_page: "https://status.supabase.com/api/v2/status.json",
        parser: ParserKind::Default,
    },
];

static INFRA_CACHE: Lazy<Mutex<Option<CacheEntry>>> = Lazy::new(|| Mutex::new(None));

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn status_order(status: &str) -> u8 {
    match status {
        "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE" => 0,
        "SERVICE_OPERATIONAL_STATUS_PARTIAL_OUTAGE" => 1,
        "SERVICE_OPERATIONAL_STATUS_DEGRADED" => 2,
        "SERVICE_OPERATIONAL_STATUS_MAINTENANCE" => 3,
        "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED" => 4,
        "SERVICE_OPERATIONAL_STATUS_OPERATIONAL" => 5,
        _ => 6,
    }
}

fn normalize_to_proto_status(raw: &str) -> String {
    if raw.trim().is_empty() {
        return "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED".to_string();
    }

    let value = raw.to_ascii_lowercase();
    if value == "none" || value == "operational" || value.contains("all systems operational") {
        "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string()
    } else if value == "minor" || value == "degraded_performance" || value.contains("degraded") {
        "SERVICE_OPERATIONAL_STATUS_DEGRADED".to_string()
    } else if value == "partial_outage" {
        "SERVICE_OPERATIONAL_STATUS_PARTIAL_OUTAGE".to_string()
    } else if value == "major"
        || value == "major_outage"
        || value == "critical"
        || value.contains("outage")
    {
        "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE".to_string()
    } else if value == "maintenance" || value.contains("maintenance") {
        "SERVICE_OPERATIONAL_STATUS_MAINTENANCE".to_string()
    } else {
        "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED".to_string()
    }
}

fn get_cached_statuses() -> Result<Option<Vec<ServiceStatus>>, AppError> {
    let cache = INFRA_CACHE
        .lock()
        .map_err(|_| AppError::Internal("infrastructure cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.statuses.clone()));
    }
    Ok(None)
}

fn set_cached_statuses(statuses: &[ServiceStatus]) -> Result<(), AppError> {
    let mut cache = INFRA_CACHE
        .lock()
        .map_err(|_| AppError::Internal("infrastructure cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        statuses: statuses.to_vec(),
        expires_at: Instant::now() + INFRA_CACHE_TTL,
    });
    Ok(())
}

fn make_unknown(
    service: ServiceDef,
    description: String,
    checked_at: i64,
    latency_ms: i64,
) -> ServiceStatus {
    ServiceStatus {
        id: service.id.to_string(),
        name: service.name.to_string(),
        status: "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED".to_string(),
        description,
        url: service.status_page.to_string(),
        checked_at,
        latency_ms,
    }
}

fn json_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64).or_else(|| {
        value
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<i64>().ok())
    })
}

fn json_string(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(|text| text.to_string())
        .unwrap_or_default()
}

fn is_html(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("<!") || trimmed.starts_with("<html")
}

async fn check_service_status(state: &AppState, service: ServiceDef) -> ServiceStatus {
    let checked_at = now_epoch_ms();

    let accept_header = if service.parser == ParserKind::Rss {
        "application/xml, text/xml"
    } else {
        "application/json, text/plain, */*"
    };

    let mut request = state
        .http_client
        .get(service.status_page)
        .header("Accept", accept_header)
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("Cache-Control", "no-cache");

    if service.parser != ParserKind::Incidentio {
        request = request.header("User-Agent", CHROME_UA);
    }

    let started = Instant::now();
    let response = match request.send().await {
        Ok(response) => response,
        Err(_) => {
            return make_unknown(service, "Request failed".to_string(), checked_at, 0);
        }
    };

    let latency_ms = started.elapsed().as_millis() as i64;

    if !response.status().is_success() {
        return make_unknown(
            service,
            format!("HTTP {}", response.status().as_u16()),
            checked_at,
            latency_ms,
        );
    }

    if service.parser == ParserKind::Gcp {
        let incidents = response.json::<Vec<Value>>().await.unwrap_or_default();
        let active = incidents
            .iter()
            .filter(|incident| {
                let end = incident.get("end");
                if end.is_none() || end.is_some_and(Value::is_null) {
                    return true;
                }
                end.and_then(Value::as_str)
                    .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                    .map(|end_time| end_time.with_timezone(&Utc) > Utc::now())
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        if active.is_empty() {
            return ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
                description: "All services operational".to_string(),
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            };
        }

        let has_high = active.iter().any(|incident| {
            incident
                .get("severity")
                .and_then(Value::as_str)
                .is_some_and(|severity| severity.eq_ignore_ascii_case("high"))
        });

        return ServiceStatus {
            id: service.id.to_string(),
            name: service.name.to_string(),
            status: if has_high {
                "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE"
            } else {
                "SERVICE_OPERATIONAL_STATUS_DEGRADED"
            }
            .to_string(),
            description: format!("{} active incident(s)", active.len()),
            url: service.status_page.to_string(),
            checked_at,
            latency_ms,
        };
    }

    if service.parser == ParserKind::Aws {
        return ServiceStatus {
            id: service.id.to_string(),
            name: service.name.to_string(),
            status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
            description: "Status page reachable".to_string(),
            url: service.status_page.to_string(),
            checked_at,
            latency_ms,
        };
    }

    if service.parser == ParserKind::Rss {
        let text = response
            .text()
            .await
            .unwrap_or_default()
            .to_ascii_lowercase();
        let has_incident = text.contains("<item>")
            && (text.contains("degradation")
                || text.contains("outage")
                || text.contains("incident"));

        return ServiceStatus {
            id: service.id.to_string(),
            name: service.name.to_string(),
            status: if has_incident {
                "SERVICE_OPERATIONAL_STATUS_DEGRADED"
            } else {
                "SERVICE_OPERATIONAL_STATUS_OPERATIONAL"
            }
            .to_string(),
            description: if has_incident {
                "Recent incidents reported"
            } else {
                "No recent incidents"
            }
            .to_string(),
            url: service.status_page.to_string(),
            checked_at,
            latency_ms,
        };
    }

    if service.parser == ParserKind::Instatus {
        let payload = response.json::<Value>().await.unwrap_or(Value::Null);
        let page_status = json_string(payload.get("page").and_then(|page| page.get("status")));

        return if page_status == "UP" {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
                description: "All systems operational".to_string(),
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else if page_status == "HASISSUES" {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_DEGRADED".to_string(),
                description: "Some issues reported".to_string(),
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else {
            make_unknown(service, page_status, checked_at, latency_ms)
        };
    }

    if service.parser == ParserKind::Statusio {
        let payload = response.json::<Value>().await.unwrap_or(Value::Null);
        let overall = payload
            .get("result")
            .and_then(|result| result.get("status_overall"));
        let code = json_i64(overall.and_then(|overall| overall.get("status_code"))).unwrap_or(0);
        let status_label = json_string(overall.and_then(|overall| overall.get("status")));

        return if code == 100 {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
                description: if status_label.is_empty() {
                    "All systems operational".to_string()
                } else {
                    status_label
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else if (300..500).contains(&code) {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_DEGRADED".to_string(),
                description: if status_label.is_empty() {
                    "Degraded performance".to_string()
                } else {
                    status_label
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else if code >= 500 {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE".to_string(),
                description: if status_label.is_empty() {
                    "Service disruption".to_string()
                } else {
                    status_label
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else {
            make_unknown(service, status_label, checked_at, latency_ms)
        };
    }

    if service.parser == ParserKind::Slack {
        let payload = response.json::<Value>().await.unwrap_or(Value::Null);
        let status = json_string(payload.get("status"));
        let incident_count = payload
            .get("active_incidents")
            .and_then(Value::as_array)
            .map(|incidents| incidents.len())
            .unwrap_or(0);

        return if status == "ok" {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
                description: "All systems operational".to_string(),
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else if status == "active" || incident_count > 0 {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_DEGRADED".to_string(),
                description: format!("{} active incident(s)", incident_count.max(1)),
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else {
            make_unknown(service, status, checked_at, latency_ms)
        };
    }

    if service.parser == ParserKind::Stripe {
        let payload = response.json::<Value>().await.unwrap_or(Value::Null);
        let large_status = json_string(payload.get("largestatus"));
        let message = json_string(payload.get("message"));

        return if large_status == "up" {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
                description: if message.is_empty() {
                    "All systems operational".to_string()
                } else {
                    message
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else if large_status == "degraded" {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_DEGRADED".to_string(),
                description: if message.is_empty() {
                    "Degraded performance".to_string()
                } else {
                    message
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else if large_status == "down" {
            ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE".to_string(),
                description: if message.is_empty() {
                    "Service disruption".to_string()
                } else {
                    message
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            }
        } else {
            make_unknown(service, message, checked_at, latency_ms)
        };
    }

    if service.parser == ParserKind::Incidentio {
        let text = response.text().await.unwrap_or_default();

        if is_html(&text) {
            let lower = text.to_ascii_lowercase();
            if lower.contains("all systems operational")
                || lower.contains("fully operational")
                || lower.contains("no issues")
            {
                return ServiceStatus {
                    id: service.id.to_string(),
                    name: service.name.to_string(),
                    status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
                    description: "All systems operational".to_string(),
                    url: service.status_page.to_string(),
                    checked_at,
                    latency_ms,
                };
            }
            if lower.contains("degraded")
                || lower.contains("partial outage")
                || lower.contains("experiencing issues")
            {
                return ServiceStatus {
                    id: service.id.to_string(),
                    name: service.name.to_string(),
                    status: "SERVICE_OPERATIONAL_STATUS_DEGRADED".to_string(),
                    description: "Some issues reported".to_string(),
                    url: service.status_page.to_string(),
                    checked_at,
                    latency_ms,
                };
            }
            return make_unknown(
                service,
                "Could not parse status".to_string(),
                checked_at,
                latency_ms,
            );
        }

        let payload = match serde_json::from_str::<Value>(&text) {
            Ok(payload) => payload,
            Err(_) => {
                return make_unknown(
                    service,
                    "Invalid response".to_string(),
                    checked_at,
                    latency_ms,
                );
            }
        };

        let indicator = json_string(
            payload
                .get("status")
                .and_then(|status| status.get("indicator")),
        );
        let description = json_string(
            payload
                .get("status")
                .and_then(|status| status.get("description")),
        );

        if indicator == "none" || description.to_ascii_lowercase().contains("operational") {
            return ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
                description: if description.is_empty() {
                    "All systems operational".to_string()
                } else {
                    description
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            };
        }

        if indicator == "minor" || indicator == "maintenance" {
            return ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_DEGRADED".to_string(),
                description: if description.is_empty() {
                    "Minor issues".to_string()
                } else {
                    description
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            };
        }

        if indicator == "major" || indicator == "critical" {
            return ServiceStatus {
                id: service.id.to_string(),
                name: service.name.to_string(),
                status: "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE".to_string(),
                description: if description.is_empty() {
                    "Major outage".to_string()
                } else {
                    description
                },
                url: service.status_page.to_string(),
                checked_at,
                latency_ms,
            };
        }

        return ServiceStatus {
            id: service.id.to_string(),
            name: service.name.to_string(),
            status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL".to_string(),
            description: if description.is_empty() {
                "Status OK".to_string()
            } else {
                description
            },
            url: service.status_page.to_string(),
            checked_at,
            latency_ms,
        };
    }

    let text = response.text().await.unwrap_or_default();
    if is_html(&text) {
        return make_unknown(
            service,
            "Blocked by service".to_string(),
            checked_at,
            latency_ms,
        );
    }

    let payload = match serde_json::from_str::<Value>(&text) {
        Ok(payload) => payload,
        Err(_) => {
            return make_unknown(
                service,
                "Invalid JSON response".to_string(),
                checked_at,
                latency_ms,
            );
        }
    };

    if payload
        .get("status")
        .and_then(|status| status.get("indicator"))
        .is_some()
    {
        let indicator = json_string(
            payload
                .get("status")
                .and_then(|status| status.get("indicator")),
        );
        let description = json_string(
            payload
                .get("status")
                .and_then(|status| status.get("description")),
        );

        return ServiceStatus {
            id: service.id.to_string(),
            name: service.name.to_string(),
            status: normalize_to_proto_status(&indicator),
            description,
            url: service.status_page.to_string(),
            checked_at,
            latency_ms,
        };
    }

    if payload
        .get("status")
        .and_then(|status| status.get("status"))
        .is_some()
    {
        let status = json_string(
            payload
                .get("status")
                .and_then(|status| status.get("status")),
        );
        let description = json_string(
            payload
                .get("status")
                .and_then(|status| status.get("description")),
        );

        return ServiceStatus {
            id: service.id.to_string(),
            name: service.name.to_string(),
            status: if status == "ok" {
                "SERVICE_OPERATIONAL_STATUS_OPERATIONAL"
            } else {
                "SERVICE_OPERATIONAL_STATUS_DEGRADED"
            }
            .to_string(),
            description,
            url: service.status_page.to_string(),
            checked_at,
            latency_ms,
        };
    }

    if payload.get("page").is_some() && payload.get("status").is_some() {
        let indicator = json_string(
            payload
                .get("status")
                .and_then(|status| status.get("indicator")),
        );
        let description = json_string(
            payload
                .get("status")
                .and_then(|status| status.get("description")),
        );

        return ServiceStatus {
            id: service.id.to_string(),
            name: service.name.to_string(),
            status: normalize_to_proto_status(if indicator.is_empty() {
                description.as_str()
            } else {
                indicator.as_str()
            }),
            description: if description.is_empty() {
                "Status available".to_string()
            } else {
                description
            },
            url: service.status_page.to_string(),
            checked_at,
            latency_ms,
        };
    }

    make_unknown(
        service,
        "Unknown format".to_string(),
        checked_at,
        latency_ms,
    )
}

pub async fn list_service_statuses(
    State(state): State<AppState>,
    Json(request): Json<ListServiceStatusesRequest>,
) -> Result<Json<ListServiceStatusesResponse>, AppError> {
    let statuses = if let Some(cached) = get_cached_statuses()? {
        cached
    } else {
        let mut tasks = Vec::with_capacity(SERVICES.len());
        for service in SERVICES {
            tasks.push(check_service_status(&state, service));
        }
        let fresh = join_all(tasks).await;
        set_cached_statuses(&fresh)?;
        fresh
    };

    let mut filtered = if !request.status.trim().is_empty()
        && request.status != "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED"
    {
        statuses
            .into_iter()
            .filter(|status| status.status == request.status)
            .collect::<Vec<_>>()
    } else {
        statuses
    };

    filtered.sort_by_key(|status| status_order(status.status.as_str()));

    Ok(Json(ListServiceStatusesResponse { statuses: filtered }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_status_strings() {
        assert_eq!(
            normalize_to_proto_status("operational"),
            "SERVICE_OPERATIONAL_STATUS_OPERATIONAL"
        );
        assert_eq!(
            normalize_to_proto_status("major_outage"),
            "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE"
        );
        assert_eq!(
            normalize_to_proto_status("maintenance"),
            "SERVICE_OPERATIONAL_STATUS_MAINTENANCE"
        );
    }

    #[test]
    fn orders_status_by_urgency() {
        assert!(
            status_order("SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE")
                < status_order("SERVICE_OPERATIONAL_STATUS_DEGRADED")
        );
        assert!(
            status_order("SERVICE_OPERATIONAL_STATUS_DEGRADED")
                < status_order("SERVICE_OPERATIONAL_STATUS_OPERATIONAL")
        );
    }

    #[test]
    fn detects_html_response_shape() {
        assert!(is_html("<!doctype html><html>"));
        assert!(is_html("<html><body></body></html>"));
        assert!(!is_html("{\"status\": \"ok\"}"));
    }
}
