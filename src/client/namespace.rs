//! Client for creating, listing, updating, and deleting ORCHER namespaces.

use std::time::Duration;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::client::workflow::AuthInterceptor;

use crate::error::{Error, Result};
use crate::proto::orcher::v1::{
    namespace_service_client::NamespaceServiceClient, CreateNamespaceRequest,
    DeleteNamespaceRequest, DeprecateNamespaceRequest, GetNamespaceRequest, ListNamespacesRequest,
    NamespaceInfo, UpdateNamespaceRequest,
};

/// Client for managing ORCHER namespaces.
///
/// Every method returns [`Error::Internal`] wrapping the
/// server's message if the RPC fails or the server replies without a
/// namespace.
#[derive(Clone)]
pub struct NamespaceClient {
    client: NamespaceServiceClient<InterceptedService<Channel, AuthInterceptor>>,
    #[allow(dead_code)]
    timeout: Duration,
}

impl NamespaceClient {
    /// Creates a client for the server at `address`, sending no credentials.
    ///
    /// The connection is lazy: it is opened on the first call, with a 5 second
    /// connect timeout and a 30 second per-request timeout.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `address` is not a valid URI.
    pub async fn connect(address: impl Into<String>) -> Result<Self> {
        let address = address.into();

        let channel = Channel::from_shared(address.clone())
            .map_err(|e| Error::configuration(format!("Invalid server address: {}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .connect_lazy();

        Ok(Self::from_channel(channel))
    }

    /// Creates a client on a pre-built gRPC channel that sends no credentials.
    ///
    /// Use [`NamespaceClient::with_auth`] against a server that requires them.
    pub fn from_channel(channel: Channel) -> Self {
        Self::with_auth(channel, AuthInterceptor::default())
    }

    /// Creates a client that sends the given credentials on every call.
    ///
    /// A server that requires an API key refuses unauthenticated namespace
    /// calls, so pass the same credentials the workflow client uses.
    pub fn with_auth(channel: Channel, auth: AuthInterceptor) -> Self {
        Self {
            client: NamespaceServiceClient::with_interceptor(channel, auth),
            timeout: Duration::from_secs(30),
        }
    }

    /// Creates a namespace with the given retention period, in days.
    pub async fn create_namespace(
        &self,
        name: impl Into<String>,
        retention_period_days: i32,
    ) -> Result<NamespaceInfo> {
        let mut client = self.client.clone();
        let response = client
            .create_namespace(CreateNamespaceRequest {
                name: name.into(),
                retention_period_days,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::internal(format!("Failed to create namespace: {}", e)))?;

        response
            .into_inner()
            .namespace
            .ok_or_else(|| Error::internal("Empty response from CreateNamespace".to_string()))
    }

    /// Creates a namespace with a description and owner as well as a
    /// retention period.
    pub async fn create_namespace_with_options(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        retention_period_days: i32,
        owner_email: impl Into<String>,
    ) -> Result<NamespaceInfo> {
        let mut client = self.client.clone();
        let response = client
            .create_namespace(CreateNamespaceRequest {
                name: name.into(),
                description: description.into(),
                retention_period_days,
                owner_email: owner_email.into(),
                ..Default::default()
            })
            .await
            .map_err(|e| Error::internal(format!("Failed to create namespace: {}", e)))?;

        response
            .into_inner()
            .namespace
            .ok_or_else(|| Error::internal("Empty response from CreateNamespace".to_string()))
    }

    /// Returns the namespace with the given name.
    pub async fn get_namespace(&self, name: impl Into<String>) -> Result<NamespaceInfo> {
        let mut client = self.client.clone();
        let response = client
            .get_namespace(GetNamespaceRequest { name: name.into() })
            .await
            .map_err(|e| Error::internal(format!("Failed to get namespace: {}", e)))?;

        response
            .into_inner()
            .namespace
            .ok_or_else(|| Error::internal("Empty response from GetNamespace".to_string()))
    }

    /// Lists one page of namespaces.
    ///
    /// Returns the page together with the total number of namespaces.
    pub async fn list_namespaces(
        &self,
        page_size: i32,
        page_offset: i32,
    ) -> Result<(Vec<NamespaceInfo>, i64)> {
        let mut client = self.client.clone();
        let response = client
            .list_namespaces(ListNamespacesRequest {
                page_size,
                page_offset,
            })
            .await
            .map_err(|e| Error::internal(format!("Failed to list namespaces: {}", e)))?;

        let inner = response.into_inner();
        Ok((inner.namespaces, inner.total_count))
    }

    /// Updates a namespace's description, owner, and retention period.
    pub async fn update_namespace(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        owner_email: impl Into<String>,
        retention_period_days: i32,
    ) -> Result<NamespaceInfo> {
        let mut client = self.client.clone();
        let response = client
            .update_namespace(UpdateNamespaceRequest {
                name: name.into(),
                description: description.into(),
                owner_email: owner_email.into(),
                retention_period_days,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::internal(format!("Failed to update namespace: {}", e)))?;

        response
            .into_inner()
            .namespace
            .ok_or_else(|| Error::internal("Empty response from UpdateNamespace".to_string()))
    }

    /// Deprecates a namespace, so the server refuses to start further
    /// workflows in it.
    pub async fn deprecate_namespace(&self, name: impl Into<String>) -> Result<NamespaceInfo> {
        let mut client = self.client.clone();
        let response = client
            .deprecate_namespace(DeprecateNamespaceRequest { name: name.into() })
            .await
            .map_err(|e| Error::internal(format!("Failed to deprecate namespace: {}", e)))?;

        response
            .into_inner()
            .namespace
            .ok_or_else(|| Error::internal("Empty response from DeprecateNamespace".to_string()))
    }

    /// Deletes a namespace.
    ///
    /// The deletion is soft: the server marks the namespace deleted rather
    /// than erasing it.
    pub async fn delete_namespace(&self, name: impl Into<String>) -> Result<()> {
        let mut client = self.client.clone();
        client
            .delete_namespace(DeleteNamespaceRequest { name: name.into() })
            .await
            .map_err(|e| Error::internal(format!("Failed to delete namespace: {}", e)))?;
        Ok(())
    }
}
