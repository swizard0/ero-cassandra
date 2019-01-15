#![type_length_limit="8388608"]

use std::{
    env,
    time::Duration,
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
    stmt,
    Value,
    Consistency,
};

use log::{info, error};

use ero::{
    Loop,
    ErrorSeverity,
    RestartStrategy,
    lode::{self, UsingResource},
};

fn main() {
    let mut args = env::args();
    let program = args.next().unwrap();
    let contact_points = args.next().unwrap();
    let keyspace = args.next().unwrap();
    let query = args.next().unwrap();

    info!("running {} with contact_points = {}, keyspace = {} and query = {}", program, contact_points, keyspace, query);

    let cluster_params = ero_cassandra::ClusterParams {
        contact_points,
        keyspace,
        ..Default::default()
    };

    pretty_env_logger::init_timed();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let executor = runtime.executor();

    let lode::Lode { resource, shutdown, } = ero_cassandra::spawn(
        &executor,
        ero_cassandra::Params {
            cluster_params,
            lode_params: lode::Params {
                name: "ero_cassandra restart_3 example",
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(2),
                },
            },
        },
    );

    let client_future = resource
        .using_resource_loop(
            (0, query),
            |session, (counter, query)| {
                let future = lazy(move || {
                    info!("performing query: {}, this is {} time", query, counter);
                    let mut stmt = stmt!(&query);
                    match stmt.set_consistency(Consistency::ONE) {
                        Ok(..) =>
                            Ok((stmt, query, counter)),
                        Err(error) => {
                            error!("error set_consistency: {:?}", error);
                            Err(ErrorSeverity::Fatal(()))
                        }
                    }
                });
                let future = future
                    .and_then(move |(stmt, query, counter)| {
                        session.execute(&stmt)
                            .then(move |result| {
                                match result {
                                    Ok(cass_result) =>
                                        Ok((cass_result, query, counter)),
                                    Err(error) => {
                                        error!("error executing statement: {:?}", error);
                                        Err(ErrorSeverity::Fatal(()))
                                    },
                                }
                            })
                    })
                    .and_then(|(cass_result, query, counter)| {
                        match cass_result.first_row() {
                            None => {
                                info!("empty response on query: {}", query);
                                Ok(Loop::Continue((counter + 1, query)))
                            },
                            Some(ref row) =>
                                match row.get_column(0) {
                                    Ok(ref value) =>
                                        if value.is_null() {
                                            info!("null column for first row");
                                            Ok(Loop::Continue((counter + 1, query)))
                                        } else {
                                            match Value::get_string(value) {
                                                Ok(data) => {
                                                    info!("column = {} for first row", data);
                                                    Ok(Loop::Continue((counter + 1, query)))
                                                },
                                                Err(error) => {
                                                    error!("error Value::get_string for row: {:?}", error);
                                                    Err(ErrorSeverity::Fatal(()))
                                                },
                                            }
                                        },
                                    Err(error) => {
                                        error!("error get_column(0) for row: {:?}", error);
                                        Err(ErrorSeverity::Fatal(()))
                                    },
                                },
                        }
                    });
                let future = future
                    .then(|query_result| {
                        match query_result {
                            Ok(Loop::Continue(state)) => {
                                info!("everything ok, triggering restart...");
                                Err(ErrorSeverity::Recoverable { state, })
                            },
                            Ok(Loop::Break(value)) =>
                                Ok((UsingResource::Lost, Loop::Break(value))),
                            Err(error) =>
                                Err(error),
                        }
                    });
                if counter < 3 {
                    Either::A(future)
                } else {
                    Either::B(result(Ok((UsingResource::Lost, Loop::Break(())))))
                }
            },
        )
        .then(move |_result| {
            shutdown.shutdown();
            Ok(())
        });
    executor.spawn(client_future);

    let _ = runtime.shutdown_on_idle().wait();
}
