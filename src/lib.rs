use futures::{
    Future,
};

use ero::{
    ErrorSeverity,
    lode::{
        self,
        Lode,
        Resource,
    },
};

pub struct ClusterParams {
    pub contact_points: String,
    pub num_threads_io: usize,
    pub keyspace: String,
}

pub struct Params<N> {
    pub cluster_params: ClusterParams,
    pub lode_params: lode::Params<N>,
}

pub fn spawn<N>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
)
    -> Lode<Session>
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

pub struct Session;
struct ConnectedCluster;

fn init(
    cluster_params: ClusterParams,
)
    -> impl Future<Item = ConnectedCluster, Error = ErrorSeverity<ClusterParams, ()>>
{

    futures::future::result(Err(ErrorSeverity::Fatal(())))
}

fn aquire(
    cluster: ConnectedCluster,
)
    -> impl Future<Item = (Session, ConnectedCluster), Error = ErrorSeverity<ClusterParams, ()>>
{

    futures::future::result(Err(ErrorSeverity::Fatal(())))
}

fn release(
    cluster: ConnectedCluster,
    maybe_session: Option<Session>,
)
    -> impl Future<Item = ConnectedCluster, Error = ErrorSeverity<ClusterParams, ()>>
{

    futures::future::result(Err(ErrorSeverity::Fatal(())))
}

fn close(
    cluster: ConnectedCluster,
)
    -> impl Future<Item = ClusterParams, Error = ()>
{

    futures::future::result(Err(()))
}
