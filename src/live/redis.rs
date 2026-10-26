//! The Redis behind the live node's cache: Nautilus persists the node's
//! orders, positions and accounts there (`RedisCacheConfig`, a
//! `CacheDatabaseFactory` from `nautilus_infrastructure`), loads them at the
//! next start and reconciles them with the venues.
//!
//! Redis runs in Docker the way the IB Gateway does (`data::IbGateway`):
//! one named container shared by every live process, started or reused,
//! restarted always, its port on the loopback only. Its data is Redis'
//! append-only file on a named volume, so it outlives the container. The
//! image is pulled beforehand (`docker pull
//! docker.io/library/redis:7.4.11-alpine`); the Docker socket comes from
//! `DOCKER_HOST`, else the local default. No password: the port is bound to
//! 127.0.0.1.

use std::collections::HashMap;

use anyhow::Context;
use bollard::{
    Docker,
    errors::Error as DockerError,
    models::{ContainerCreateBody, HostConfig, PortBinding, RestartPolicy, RestartPolicyNameEnum},
    query_parameters::{CreateContainerOptions, StartContainerOptions},
};
use nautilus_infrastructure::redis::cache::RedisCacheConfig;

const CONTAINER: &str = "helix-redis";
/// Redis 7.4: Nautilus needs 6.2 or later.
const IMAGE: &str = "docker.io/library/redis:7.4.11-alpine";
const VOLUME: &str = "helix-redis-data";
const HOST: &str = "127.0.0.1";
const PORT: u16 = 6379;

/// Starts the Redis container, or reuses it, and returns the cache database
/// on it. Nautilus' connection retries while Redis comes up.
///
/// # Errors
///
/// Returns an error if Docker is unreachable, or if the container cannot
/// be inspected, created (the image not pulled, the port taken) or started.
pub(super) async fn start() -> anyhow::Result<RedisCacheConfig> {
    let docker = Docker::connect_with_local_defaults()
        .context("connecting to Docker (DOCKER_HOST, else the local socket)")?;
    match docker.inspect_container(CONTAINER, None).await {
        Ok(container) => {
            let running = container
                .state
                .and_then(|state| state.running)
                .unwrap_or(false);
            if running {
                log::info!("Reusing the Redis container {CONTAINER}");
            } else {
                log::info!("Starting the Redis container {CONTAINER}");
                start_container(&docker).await?;
            }
        }
        Err(DockerError::DockerResponseServerError {
            status_code: 404, ..
        }) => {
            log::info!("Creating the Redis container {CONTAINER} from {IMAGE}");
            let options = CreateContainerOptions {
                name: Some(CONTAINER.to_string()),
                ..Default::default()
            };
            docker
                .create_container(Some(options), container())
                .await
                .with_context(|| {
                    format!("creating {CONTAINER} (is {IMAGE} pulled? is port {PORT} free?)")
                })?;
            start_container(&docker).await?;
        }
        Err(e) => return Err(e).with_context(|| format!("inspecting {CONTAINER}")),
    }
    Ok(cache_database())
}

async fn start_container(docker: &Docker) -> anyhow::Result<()> {
    docker
        .start_container(CONTAINER, None::<StartContainerOptions>)
        .await
        .with_context(|| format!("starting {CONTAINER}"))
}

/// The container: the pinned image writing its append-only file to the
/// named volume, the port on the loopback only, restarted always.
fn container() -> ContainerCreateBody {
    let port_bindings = HashMap::from([(
        format!("{PORT}/tcp"),
        Some(vec![PortBinding {
            host_ip: Some(HOST.to_string()),
            host_port: Some(PORT.to_string()),
        }]),
    )]);
    ContainerCreateBody {
        image: Some(IMAGE.to_string()),
        cmd: Some(vec![
            "redis-server".to_string(),
            "--appendonly".to_string(),
            "yes".to_string(),
        ]),
        host_config: Some(HostConfig {
            port_bindings: Some(port_bindings),
            binds: Some(vec![format!("{VOLUME}:/data")]),
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::ALWAYS),
                maximum_retry_count: None,
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The node's cache database: the container's Redis, Nautilus' defaults
/// otherwise.
fn cache_database() -> RedisCacheConfig {
    RedisCacheConfig {
        host: Some(HOST.to_string()),
        port: Some(PORT),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_keeps_its_data_in_an_append_only_file_on_the_volume() {
        let container = container();
        assert_eq!(
            container.cmd.unwrap(),
            ["redis-server", "--appendonly", "yes"]
        );
        let host = container.host_config.unwrap();
        // Redis' working directory, where the append-only file goes.
        assert_eq!(host.binds.unwrap(), ["helix-redis-data:/data"]);
    }

    #[test]
    fn the_container_restarts_always() {
        let host = container().host_config.unwrap();
        assert_eq!(
            host.restart_policy.unwrap().name,
            Some(RestartPolicyNameEnum::ALWAYS)
        );
    }

    #[test]
    fn the_port_is_bound_to_the_loopback_only() {
        let bindings = container().host_config.unwrap().port_bindings.unwrap();
        let [binding] = bindings["6379/tcp"].as_deref().unwrap() else {
            panic!("{bindings:?}")
        };
        assert_eq!(binding.host_ip.as_deref(), Some("127.0.0.1"));
        assert_eq!(binding.host_port.as_deref(), Some("6379"));
    }

    #[test]
    fn the_cache_database_is_the_containers_redis_without_credentials() {
        let config = cache_database();
        assert_eq!(config.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(config.port, Some(6379));
        assert!(config.username.is_none() && config.password.is_none());
    }
}
