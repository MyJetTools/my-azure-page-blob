use std::sync::Arc;

use my_azure_storage_sdk::{blob::BlobProperties, AzureStorageConnection, AzureStorageError};

use async_trait::async_trait;

use super::MyPageBlob;

pub struct MyAzurePageBlob {
    connection: Arc<AzureStorageConnection>,
    container_name: String,
    blob_name: String,
}

impl MyAzurePageBlob {
    pub fn new(
        connection: Arc<AzureStorageConnection>,
        container_name: String,
        blob_name: String,
    ) -> Self {
        Self {
            connection,
            container_name,
            blob_name,
        }
    }
}

#[async_trait]
impl MyPageBlob for MyAzurePageBlob {
    fn get_blob_name(&self) -> &str {
        return self.blob_name.as_str();
    }

    fn get_container_name(&self) -> &str {
        return self.container_name.as_str();
    }

    async fn resize(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::page_blob::sdk::resize_page_blob(
            self.connection.as_ref(),
            self.container_name.as_str(),
            self.blob_name.as_str(),
            pages_amount,
        )
        .await?;

        Ok(())
    }

    async fn create_container_if_not_exist(&self) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::blob_container::sdk::create_container_if_not_exist(
            self.connection.as_ref(),
            self.container_name.as_str(),
        )
        .await
    }

    async fn get_available_pages_amount(&self) -> Result<usize, AzureStorageError> {
        let props = my_azure_storage_sdk::blob::sdk::get_blob_properties(
            self.connection.as_ref(),
            self.container_name.as_str(),
            self.blob_name.as_str(),
        )
        .await?;

        Ok(props.blob_size / my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE)
    }

    async fn create(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::page_blob::sdk::create_page_blob(
            self.connection.as_ref(),
            self.container_name.as_str(),
            &self.blob_name,
            pages_amount,
        )
        .await
    }

    async fn create_if_not_exists(&self, pages_amount: usize) -> Result<usize, AzureStorageError> {
        let props = my_azure_storage_sdk::page_blob::sdk::create_page_blob_if_not_exists(
            self.connection.as_ref(),
            self.container_name.as_str(),
            &self.blob_name,
            pages_amount,
        )
        .await?;

        Ok(props.blob_size / my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE)
    }

    async fn get(
        &self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        my_azure_storage_sdk::page_blob::sdk::get_pages(
            self.connection.as_ref(),
            self.container_name.as_str(),
            self.blob_name.as_str(),
            start_page_no,
            pages_amount,
        )
        .await
    }

    async fn save_pages(
        &self,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::page_blob::sdk::save_pages(
            self.connection.as_ref(),
            self.container_name.as_str(),
            self.blob_name.as_str(),
            start_page_no,
            payload,
        )
        .await
    }

    async fn delete(&self) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::blob::sdk::delete_blob(
            self.connection.as_ref(),
            self.container_name.as_str(),
            self.blob_name.as_str(),
        )
        .await
    }

    async fn delete_if_exists(&self) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::blob::sdk::delete_blob_if_exists(
            self.connection.as_ref(),
            self.container_name.as_str(),
            self.blob_name.as_str(),
        )
        .await
    }

    async fn download(&self) -> Result<Vec<u8>, AzureStorageError> {
        my_azure_storage_sdk::blob::sdk::download_blob(
            self.connection.as_ref(),
            self.container_name.as_str(),
            self.blob_name.as_str(),
        )
        .await
    }

    async fn get_blob_properties(&self) -> Result<BlobProperties, AzureStorageError> {
        my_azure_storage_sdk::blob::sdk::get_blob_properties(
            self.connection.as_ref(),
            self.container_name.as_ref(),
            self.blob_name.as_ref(),
        )
        .await
    }
}
