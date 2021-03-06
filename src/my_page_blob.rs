use my_azure_storage_sdk::{blob::BlobProperties, AzureStorageError};

use async_trait::async_trait;

#[async_trait]
pub trait MyPageBlob {
    fn get_container_name(&self) -> &str;
    fn get_blob_name(&self) -> &str;

    async fn get_blob_properties(&mut self) -> Result<BlobProperties, AzureStorageError>;

    async fn create(&mut self, pages_amount: usize) -> Result<(), AzureStorageError>;
    async fn create_if_not_exists(&mut self, pages_amount: usize) -> Result<(), AzureStorageError>;
    async fn get_available_pages_amount(&mut self) -> Result<usize, AzureStorageError>;
    async fn create_container_if_not_exist(&mut self) -> Result<(), AzureStorageError>;

    async fn resize(&mut self, pages_amount: usize) -> Result<(), AzureStorageError>;

    async fn delete(&mut self) -> Result<(), AzureStorageError>;

    async fn delete_if_exists(&mut self) -> Result<(), AzureStorageError>;

    async fn get(
        &mut self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError>;

    async fn save_pages(
        &mut self,
        start_page_no: usize,
        max_pages_to_write: usize,
        mut payload: Vec<u8>,
    ) -> Result<usize, AzureStorageError>;

    async fn auto_ressize_and_save_pages(
        &mut self,
        start_page_no: usize,
        max_pages_to_write_single_round_trip: usize,
        mut payload: Vec<u8>,
        resize_pages_ration: usize,
    ) -> Result<usize, AzureStorageError>;

    async fn download(&mut self) -> Result<Vec<u8>, AzureStorageError>;
}
