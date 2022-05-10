use crate::Cluster;
use crate::cassandra::session::Session;
use crate::cassandra::error::*;

use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::thread;

use std::sync::mpsc::channel;

/// find or initialize several clusters/sessions inside an Arc mutex
/// should not be confunded with max connections, this is much more related to simultaneous shared sessions
/// e.g you can have 2 cluster sessions each with 10 connections running 1000+ queries
/// this struct simplifies the need to create and handle multiple sessions
pub struct ClusterMutex {
    mutex_cluster: Arc<Vec<Mutex<Cluster>>>,
    max_concurrency: usize,
}

impl ClusterMutex {
    /// create several cluster sessions based on the number of concurrencies
    pub fn new(max_concurrency: usize, cluster: Cluster) -> Self {
        let clusters: Vec<Mutex<Cluster>> = vec![Mutex::new(cluster)];
        let mutex_cluster = Arc::new(clusters);

        ClusterMutex {
            mutex_cluster,
            max_concurrency
        }
    }

    /// gathers the first session cluster available and connect to it
    /// locking it until drop
    pub fn connect(&self) -> Result<Session> {
        let mutex_reference = &self.mutex_cluster.deref();
        let first_cluster = mutex_reference.first().unwrap();
        let mut mutex_cluster = first_cluster.lock().unwrap();
        mutex_cluster.connect()
    }
}
