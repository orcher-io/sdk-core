//! Client for creating, listing, updating, and deleting ORCHER namespaces.

use crate::client::connection::{ClientChannel, Connection};

use crate::error::{Error, Result};
use crate::types::Namespace;
use orcher_proto::orcher::v1::{
    namespace_service_client::NamespaceServiceClient, CreateNamespaceRequest,
    DeleteNamespaceRequest, DeprecateNamespaceRequest, GetNamespaceRequest, ListNamespacesRequest,
    UpdateNamespaceRequest,
};

/// Client for managing ORCHER namespaces.
///
/// A call the engine refuses fails with its status ([`Error::Status`]), so
/// a caller can tell a namespace that already exists from credentials the
/// engine does not accept. A reply without a namespace fails with
/// [`Error::Internal`].
#[derive(Clone)]
pub struct NamespaceClient {
    client: NamespaceServiceClient<ClientChannel>,
}

impl NamespaceClient {
    /// A client on `connection`.
    pub(crate) fn new(connection: &Connection) -> Self {
        Self {
            client: NamespaceServiceClient::new(connection.authenticated()),
        }
    }

    /// Creates a namespace with the given retention period, in days.
    pub async fn create_namespace(
        &self,
        name: impl Into<String>,
        retention_period_days: i32,
    ) -> Result<Namespace> {
        let mut client = self.client.clone();
        let response = client
            .create_namespace(CreateNamespaceRequest {
                name: name.into(),
                retention_period_days,
                ..Default::default()
            })
            .await
            .map_err(crate::wire::status::error)?;

        response
            .into_inner()
            .namespace
            .map(crate::wire::client::namespace)
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
    ) -> Result<Namespace> {
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
            .map_err(crate::wire::status::error)?;

        response
            .into_inner()
            .namespace
            .map(crate::wire::client::namespace)
            .ok_or_else(|| Error::internal("Empty response from CreateNamespace".to_string()))
    }

    /// Returns the namespace with the given name.
    pub async fn get_namespace(&self, name: impl Into<String>) -> Result<Namespace> {
        let mut client = self.client.clone();
        let response = client
            .get_namespace(GetNamespaceRequest {
                name: name.into(),
                ..Default::default()
            })
            .await
            .map_err(crate::wire::status::error)?;

        response
            .into_inner()
            .namespace
            .map(crate::wire::client::namespace)
            .ok_or_else(|| Error::internal("Empty response from GetNamespace".to_string()))
    }

    /// Lists one page of namespaces.
    ///
    /// Returns the page together with the total number of namespaces.
    pub async fn list_namespaces(
        &self,
        page_size: i32,
        page_offset: i32,
    ) -> Result<(Vec<Namespace>, i64)> {
        let mut client = self.client.clone();
        let response = client
            .list_namespaces(ListNamespacesRequest {
                page_size,
                page_offset,
                ..Default::default()
            })
            .await
            .map_err(crate::wire::status::error)?;

        let inner = response.into_inner();
        Ok((
            inner
                .namespaces
                .into_iter()
                .map(crate::wire::client::namespace)
                .collect(),
            inner.total_count,
        ))
    }

    /// Updates a namespace's description, owner, and retention period.
    pub async fn update_namespace(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        owner_email: impl Into<String>,
        retention_period_days: i32,
    ) -> Result<Namespace> {
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
            .map_err(crate::wire::status::error)?;

        response
            .into_inner()
            .namespace
            .map(crate::wire::client::namespace)
            .ok_or_else(|| Error::internal("Empty response from UpdateNamespace".to_string()))
    }

    /// Deprecates a namespace, so the server refuses to start further
    /// workflows in it.
    pub async fn deprecate_namespace(&self, name: impl Into<String>) -> Result<Namespace> {
        let mut client = self.client.clone();
        let response = client
            .deprecate_namespace(DeprecateNamespaceRequest {
                name: name.into(),
                ..Default::default()
            })
            .await
            .map_err(crate::wire::status::error)?;

        response
            .into_inner()
            .namespace
            .map(crate::wire::client::namespace)
            .ok_or_else(|| Error::internal("Empty response from DeprecateNamespace".to_string()))
    }

    /// Deletes a namespace.
    ///
    /// The deletion is soft: the server marks the namespace deleted rather
    /// than erasing it.
    pub async fn delete_namespace(&self, name: impl Into<String>) -> Result<()> {
        let mut client = self.client.clone();
        client
            .delete_namespace(DeleteNamespaceRequest {
                name: name.into(),
                ..Default::default()
            })
            .await
            .map_err(crate::wire::status::error)?;
        Ok(())
    }
}
