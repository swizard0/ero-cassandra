use std::{
    sync::Arc,
    ops::Deref,
};

use futures::{
    Future,
    future::{
        lazy,
        result,
        Either,
    },
};

use cassandra_cpp::{
    Cluster,
    Session,
};

use log::{
    debug,
    error,
};

use ero::{
    ErrorSeverity,
    lode::{self, Lode},
};

pub struct ClusterParams {
    pub contact_points: String,
    pub keyspace: String,
    pub num_threads_io: usize,
    pub queue_size_io: usize,
    pub queue_size_event: usize,
    pub core_connections_per_host: usize,
    pub max_connections_per_host: usize,
    pub max_concurrent_creation: usize,
    pub max_requests_per_flush: usize,
    pub write_bytes_high_water_mark: usize,
    pub pending_requests_high_water_mark: usize,
    pub load_balance_round_robin: bool,
    pub token_aware_routing: bool,
    pub use_schema: bool,
}

impl Default for ClusterParams {
    fn default() -> ClusterParams {
        ClusterParams {
            contact_points: "127.0.0.1".to_string(),
            keyspace: "default".to_string(),
            num_threads_io: 2,
            queue_size_io: 16384,
            queue_size_event: 32768,
            core_connections_per_host: 2,
            max_connections_per_host: 4,
            max_concurrent_creation: 2,
            max_requests_per_flush: 256,
            write_bytes_high_water_mark: 1024 * 1024,
            pending_requests_high_water_mark: 512,
            load_balance_round_robin: true,
            token_aware_routing: false,
            use_schema: false,
        }
    }
}

pub struct Params<N> {
    pub cluster_params: ClusterParams,
    pub lode_params: ero::Params<N>,
}

pub struct SharedSession {
    session: Arc<Session>,
}

impl Deref for SharedSession {
    type Target = Session;

    fn deref(&self) -> &Self::Target {
        &*self.session
    }
}

pub fn spawn<N>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
)
    -> Lode<SharedSession>
where N: AsRef<str> + Send + 'static,
{
    let Params { cluster_params, lode_params, } = params;

    lode::shared::spawn(
        executor,
        lode_params,
        cluster_params,
        init,
        aquire,
        release,
        close,
    )
}

struct ConnectedCluster {
    session: SharedSession,
    _cluster: Cluster,
    params: ClusterParams,
}

fn init(
    params: ClusterParams,
)
    -> Box<dyn Future<Item = ConnectedCluster, Error = ErrorSeverity<ClusterParams, ()>> + Send + 'static>
{
    let future = lazy(move || {
        let mut cluster = Cluster::default();
        debug!("setting contact points: {:?} and configuring cluster", params.contact_points);
        let config_result = cluster.set_contact_points(&params.contact_points)
            .map_err(|error| {
                error!("error setting contact_points: {:?}", error);
                ErrorSeverity::Recoverable { state: (), }
            })
            .and_then(|cluster| {
                cluster.set_num_threads_io(params.num_threads_io as u32)
                    .map_err(|error| {
                        error!("error setting num_threads_io: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .and_then(|cluster| {
                cluster.set_queue_size_io(params.queue_size_io as u32)
                    .map_err(|error| {
                        error!("error setting queue_size_io: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .and_then(|cluster| {
                cluster.set_queue_size_event(params.queue_size_event as u32)
                    .map_err(|error| {
                        error!("error setting queue_size_event: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .and_then(|cluster| {
                cluster.set_core_connections_per_host(params.core_connections_per_host as u32)
                    .map_err(|error| {
                        error!("error setting core_connections_per_host: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .and_then(|cluster| {
                cluster.set_max_connections_per_host(params.max_connections_per_host as u32)
                    .map_err(|error| {
                        error!("error setting max_connections_per_host: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .and_then(|cluster| {
                cluster.set_max_concurrent_creation(params.max_concurrent_creation as u32)
                    .map_err(|error| {
                        error!("error setting max_concurrent_creation: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .and_then(|cluster| {
                cluster.set_max_requests_per_flush(params.max_requests_per_flush as u32)
                    .map_err(|error| {
                        error!("error setting max_requests_per_flush: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .and_then(|cluster| {
                cluster.set_write_bytes_high_water_mark(params.write_bytes_high_water_mark as u32)
                    .map_err(|error| {
                        error!("error setting write_bytes_high_water_mark: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .and_then(|cluster| {
                cluster.set_pending_requests_high_water_mark(params.pending_requests_high_water_mark as u32)
                    .map_err(|error| {
                        error!("error setting pending_requests_high_water_mark: {:?}", error);
                        ErrorSeverity::Fatal(())
                    })
            })
            .map(|cluster| {
                if params.load_balance_round_robin {
                    cluster.set_load_balance_round_robin();
                }
                cluster.set_token_aware_routing(params.token_aware_routing);
                cluster.set_use_schema(params.use_schema);
            });
        match config_result {
            Ok(()) =>
                Ok((cluster, params)),
            Err(ErrorSeverity::Recoverable { state: (), }) =>
                Err(ErrorSeverity::Recoverable { state: params, }),
            Err(ErrorSeverity::Fatal(())) =>
                Err(ErrorSeverity::Fatal(())),
        }
    });
    let future = future
        .and_then(|(cluster, params)| {
            debug!("setting keyspace {:?} and connecting to cluster", params.keyspace);
            let session = Session::new();
            match session.connect_keyspace(&cluster, &params.keyspace) {
                Ok(connect_future) => {
                    let future = connect_future
                        .then(move |connect_result| {
                            match connect_result {
                                Ok(()) =>
                                    Ok(ConnectedCluster {
                                        session: SharedSession { session: Arc::new(session), },
                                        _cluster: cluster,
                                        params,
                                    }),
                                Err(error) => {
                                    error!("error connect_future: {:?}", error);
                                    Err(ErrorSeverity::Recoverable { state: params, })
                                },
                            }
                        });
                    Either::A(future)
                },
                Err(error) => {
                    error!("error connect_keyspace: {:?}", error);
                    Either::B(result(Err(ErrorSeverity::Recoverable { state: params, })))
                },
            }
        });
    Box::new(future)
}

fn aquire(
    connected: ConnectedCluster,
)
    -> impl Future<Item = (SharedSession, ConnectedCluster), Error = ErrorSeverity<ClusterParams, ()>>
{
    result(Ok((SharedSession { session: connected.session.session.clone(), }, connected)))
}

fn release(
    connected: ConnectedCluster,
    _maybe_session: Option<SharedSession>,
)
    -> impl Future<Item = ConnectedCluster, Error = ErrorSeverity<ClusterParams, ()>>
{
    result(Ok(connected))
}

fn close(
    connected: ConnectedCluster,
)
    -> impl Future<Item = ClusterParams, Error = ()>
{
    result(Ok(connected.params))
}
