use my_azure_storage_sdk::{blob::BlobProperties, AzureStorageConnection, AzureStorageError};

use async_trait::async_trait;

use crate::sdk::MyAzurePageBlobSdk;

use super::MyPageBlob;

pub struct MyAzurePageBlob {
    sdk: MyAzurePageBlobSdk,
    connection: AzureStorageConnection,
}

impl MyAzurePageBlob {
    pub fn new(
        connection: AzureStorageConnection,
        container_name: String,
        blob_name: String,
    ) -> Self {
        Self {
            sdk: MyAzurePageBlobSdk::new(container_name, blob_name),
            connection,
        }
    }
}

#[async_trait]
impl MyPageBlob for MyAzurePageBlob {
    fn get_blob_name(&self) -> &str {
        return self.sdk.blob_name.as_str();
    }

    fn get_container_name(&self) -> &str {
        return self.sdk.container_name.as_str();
    }

    async fn resize(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.sdk.resize(&self.connection, pages_amount).await
    }

    async fn create_container_if_not_exist(&mut self) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .create_container_if_not_exist(&self.connection)
            .await;
    }

    async fn get_available_pages_amount(&mut self) -> Result<usize, AzureStorageError> {
        return self.sdk.get_available_pages_amount(&self.connection).await;
    }

    async fn create(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        return self.sdk.create(&self.connection, pages_amount).await;
    }

    async fn create_if_not_exists(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        return self
            .sdk
            .create_if_not_exists(&self.connection, pages_amount)
            .await;
    }

    async fn get(
        &mut self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        return self
            .sdk
            .get(&self.connection, start_page_no, pages_amount)
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
            .save_pages(&self.connection, start_page_no, max_pages_to_write, payload)
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
                &self.connection,
                start_page_no,
                max_pages_to_write_single_round_trip,
                payload,
                resize_pages_ratio,
            )
            .await;
    }

    async fn delete(&mut self) -> Result<(), AzureStorageError> {
        return self.sdk.delete(&self.connection).await;
    }

    async fn delete_if_exists(&mut self) -> Result<(), AzureStorageError> {
        return self.sdk.delete_if_exists(&self.connection).await;
    }

    async fn download(&mut self) -> Result<Vec<u8>, AzureStorageError> {
        return self.sdk.download(&self.connection).await;
    }

    async fn get_blob_properties(&mut self) -> Result<BlobProperties, AzureStorageError> {
        return self.sdk.get_blob_properties(&self.connection).await;
    }
}
