use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone)]
pub struct Admission {
    client: reqwest::Client,
    base: String,
    token: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Scope {
    pub actor_id: String,
    pub channel_id: String,
    pub kind: String,
    pub owner_id: String,
    pub runner_id: String,
    pub device_id: String,
    pub generation: String,
    pub source_id: String,
    pub action: String,
}

impl Admission {
    pub fn new(base: String, token: String) -> Result<Self> {
        if base.is_empty() || token.len() < 32 {
            bail!("media admission URL and a control token of at least 32 bytes are required");
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            base,
            token,
        })
    }

    pub async fn admit(&self, ticket: &str, runner: &str, action: &str) -> Result<Scope> {
        if ticket.is_empty() || ticket.len() > 4096 || runner.is_empty() || runner.len() > 128 {
            bail!("media ticket required");
        }
        let response = self
            .client
            .post(format!("{}/admit", self.base))
            .bearer_auth(&self.token)
            .json(&serde_json::json!({"ticket":ticket,"runner_id":runner,"action":action}))
            .send()
            .await
            .context("media admission unavailable")?;
        if !response.status().is_success() {
            bail!("media admission denied");
        }
        let scope: Scope = response
            .json()
            .await
            .context("invalid media admission response")?;
        if scope.runner_id != runner
            || scope.action != action
            || scope.source_id != "screen"
            || scope.kind != "screen"
            || !scope
                .generation
                .parse::<u64>()
                .is_ok_and(|value| value > 0 && value.to_string() == scope.generation)
        {
            bail!("media admission scope mismatch");
        }
        Ok(scope)
    }

    pub async fn check_lease(&self, scope: &Scope) -> Result<(), LeaseFailure> {
        let response = self
            .client
            .post(format!("{}/lease", self.base))
            .bearer_auth(&self.token)
            .json(scope)
            .send()
            .await
            .map_err(|_| LeaseFailure::Unavailable)?;
        match response.status().as_u16() {
            200..=299 => Ok(()),
            401 | 403 => Err(LeaseFailure::Denied),
            _ => Err(LeaseFailure::Unavailable),
        }
    }
}

#[derive(Debug)]
pub enum LeaseFailure {
    Denied,
    Unavailable,
}
