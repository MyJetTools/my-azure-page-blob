use my_azure_storage_sdk::{AzureStorageConnection, AzureStorageError};

use async_trait::async_trait;
use my_telemetry::MyTelemetryToConsole;

use crate::sdk::MyAzurePageBlobSdk;

use super::MyPageBlob;

pub struct MyAzurePageBlob<TConnection: AzureStorageConnection + Send + Sync + 'static> {
    sdk: MyAzurePageBlobSdk,
    connection: TConnection,
}

impl<TConnection: AzureStorageConnection + Send + Sync + 'static> MyAzurePageBlob<TConnection> {
    pub fn new(connection: TConnection, container_name: String, blob_name: String) -> Self {
        Self {
            sdk: MyAzurePageBlobSdk::new(container_name, blob_name),
            connection,
        }
    }
}

#[async_trait]
impl<TConnection: AzureStorageConnection + Send + Sync + 'static> MyPageBlob
    for MyAzurePageBlob<TConnection>
{
    fn get_blob_name(&self) -> &str {
        return self.sdk.blob_name.as_str();
    }

    fn get_container_name(&self) -> &str {
        return self.sdk.container_name.as_str();
    }

    async fn resize(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.sdk
            .resize::<MyTelemetryToConsole>(
                self.connection.get_conneciton_info(),
                None,
                pages_amount,
            )
            .await
    }

    async fn create_container_if_not_exist(&mut self) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .create_container_if_not_exist::<MyTelemetryToConsole>(
                self.connection.get_conneciton_info(),
                None,
            )
            .await;
    }

    async fn get_available_pages_amount(&mut self) -> Result<usize, AzureStorageError> {
        return self
            .sdk
            .get_available_pages_amount::<MyTelemetryToConsole>(
                self.connection.get_conneciton_info(),
                None,
            )
            .await;
    }

    async fn create(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .create::<MyTelemetryToConsole>(
                self.connection.get_conneciton_info(),
                None,
                pages_amount,
            )
            .await;
    }

    async fn create_if_not_exists(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .create_if_not_exists::<MyTelemetryToConsole>(
                self.connection.get_conneciton_info(),
                None,
                pages_amount,
            )
            .await;
    }

    async fn get(
        &mut self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        return self
            .sdk
            .get::<MyTelemetryToConsole>(
                self.connection.get_conneciton_info(),
                None,
                start_page_no,
                pages_amount,
            )
            .await;
    }

    async fn save_pages(
        &mut self,
        start_page_no: usize,
        max_pages_to_write: usize,
        payload: Vec<u8>,
    ) -> Result<usize, AzureStorageError> {
        return self
            .sdk
            .save_pages::<MyTelemetryToConsole>(
                self.connection.get_conneciton_info(),
                None,
                start_page_no,
                max_pages_to_write,
                payload,
            )
            .await;
    }

    async fn auto_ressize_and_save_pages(
        &mut self,
        start_page_no: usize,
        max_pages_to_write_single_round_trip: usize,
        payload: Vec<u8>,
        resize_pages_ratio: usize,
    ) -> Result<usize, AzureStorageError> {
        return self
            .sdk
            .auto_ressize_and_save_pages::<MyTelemetryToConsole>(
                self.connection.get_conneciton_info(),
                None,
                start_page_no,
                max_pages_to_write_single_round_trip,
                payload,
                resize_pages_ratio,
            )
            .await;
    }

    async fn delete(&mut self) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .delete::<MyTelemetryToConsole>(self.connection.get_conneciton_info(), None)
            .await;
    }

    async fn delete_if_exists(&mut self) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .delete_if_exists::<MyTelemetryToConsole>(self.connection.get_conneciton_info(), None)
            .await;
    }

    async fn download(&mut self) -> Result<Vec<u8>, AzureStorageError> {
        return self
            .sdk
            .download::<MyTelemetryToConsole>(self.connection.get_conneciton_info(), None)
            .await;
    }
}
