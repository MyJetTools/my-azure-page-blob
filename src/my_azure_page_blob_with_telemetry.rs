use std::sync::Arc;

use my_azure_storage_sdk::{AzureStorageConnection, AzureStorageError};

use async_trait::async_trait;
use my_telemetry::MyTelemetry;

use crate::sdk::MyAzurePageBlobSdk;

use super::MyPageBlob;

pub struct MyAzurePageBlobWithTelemetry<
    TConnection: AzureStorageConnection + Send + Sync + 'static,
    TMyTelemetry: MyTelemetry + Send + Sync + 'static,
> {
    sdk: MyAzurePageBlobSdk,
    connection: TConnection,
    my_telemetry: Option<Arc<TMyTelemetry>>,
}

impl<
        TConnection: AzureStorageConnection + Send + Sync + 'static,
        TMyTelemetry: MyTelemetry + Send + Sync + 'static,
    > MyAzurePageBlobWithTelemetry<TConnection, TMyTelemetry>
{
    pub fn new(connection: TConnection, container_name: String, blob_name: String) -> Self {
        let my_telemetry = connection.get_telemetry();
        Self {
            sdk: MyAzurePageBlobSdk::new(container_name, blob_name),
            connection,
            my_telemetry,
        }
    }
}

#[async_trait]
impl<
        TConnection: AzureStorageConnection + Send + Sync + 'static,
        TMyTelemetry: MyTelemetry + Send + Sync + 'static,
    > MyPageBlob for MyAzurePageBlobWithTelemetry<TConnection, TMyTelemetry>
{
    fn get_blob_name(&self) -> &str {
        return self.sdk.blob_name.as_str();
    }

    fn get_container_name(&self) -> &str {
        return self.sdk.container_name.as_str();
    }

    async fn resize(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.sdk
            .resize(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
                pages_amount,
            )
            .await
    }

    async fn create_container_if_not_exist(&mut self) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .create_container_if_not_exist(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
            )
            .await;
    }

    async fn get_available_pages_amount(&mut self) -> Result<usize, AzureStorageError> {
        return self
            .sdk
            .get_available_pages_amount(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
            )
            .await;
    }

    async fn create(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .create(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
                pages_amount,
            )
            .await;
    }

    async fn create_if_not_exists(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .create_if_not_exists(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
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
            .get(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
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
            .save_pages(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
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
            .auto_ressize_and_save_pages(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
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
            .delete(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
            )
            .await;
    }

    async fn delete_if_exists(&mut self) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .delete_if_exists(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
            )
            .await;
    }

    async fn download(&mut self) -> Result<Vec<u8>, AzureStorageError> {
        return self
            .sdk
            .download(
                self.connection.get_conneciton_info(),
                self.my_telemetry.clone(),
            )
            .await;
    }
}
