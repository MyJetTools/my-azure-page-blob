use async_trait::async_trait;
use my_azure_storage_sdk::{
    blob::BlobProperties, page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError,
};
use tokio::sync::RwLock;

use super::MyPageBlob;

struct PageBlobData {
    pub pages: Vec<[u8; BLOB_PAGE_SIZE]>,
    pub container_created: bool,
    pub blob_created: bool,
}

impl PageBlobData {
    fn check_if_container_exists(&self) -> Result<(), AzureStorageError> {
        if self.container_created {
            return Ok(());
        }

        Err(AzureStorageError::ContainerNotFound)
    }

    fn add_new_page(&mut self) {
        let new_page = [0u8; BLOB_PAGE_SIZE];
        self.pages.push(new_page);
    }

    fn check_if_blob_exists(&self) -> Result<(), AzureStorageError> {
        self.check_if_container_exists()?;

        if self.blob_created {
            return Ok(());
        }

        Err(AzureStorageError::BlobNotFound)
    }
}

pub struct MyPageBlobMock {
    data: RwLock<PageBlobData>,
}

impl MyPageBlobMock {
    pub fn new() -> Self {
        let data = PageBlobData {
            pages: Vec::new(),
            container_created: false,
            blob_created: false,
        };
        Self {
            data: RwLock::new(data),
        }
    }
}

#[async_trait]
impl MyPageBlob for MyPageBlobMock {
    fn get_container_name(&self) -> &str {
        return "Mock_CONTAINER";
    }

    fn get_blob_name(&self) -> &str {
        return "Mock_BLOB";
    }

    async fn create(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        let mut write_access = self.data.write().await;
        write_access.check_if_container_exists()?;

        write_access.blob_created = true;

        while write_access.pages.len() < pages_amount {
            write_access.add_new_page();
        }
        Ok(())
    }

    async fn create_if_not_exists(&self, pages_amount: usize) -> Result<usize, AzureStorageError> {
        let mut write_access = self.data.write().await;
        write_access.check_if_container_exists()?;

        write_access.blob_created = true;

        while write_access.pages.len() < pages_amount {
            write_access.add_new_page();
        }
        Ok(write_access.pages.len())
    }

    async fn get_available_pages_amount(&self) -> Result<usize, AzureStorageError> {
        let read_access = self.data.read().await;
        read_access.check_if_blob_exists()?;
        Ok(read_access.pages.len())
    }

    async fn create_container_if_not_exist(&self) -> Result<(), AzureStorageError> {
        let mut write_access = self.data.write().await;
        write_access.container_created = true;
        Ok(())
    }

    async fn resize(&self, pages_amount: usize) -> Result<(), AzureStorageError> {
        let mut write_access = self.data.write().await;
        write_access.check_if_blob_exists()?;

        while write_access.pages.len() < pages_amount {
            write_access.add_new_page();
        }

        while write_access.pages.len() > pages_amount {
            let index = write_access.pages.len() - 1;
            write_access.pages.remove(index);
        }

        Ok(())
    }

    async fn delete(&self) -> Result<(), AzureStorageError> {
        let mut write_access = self.data.write().await;
        write_access.check_if_blob_exists()?;

        write_access.blob_created = false;
        return Ok(());
    }

    async fn delete_if_exists(&self) -> Result<(), AzureStorageError> {
        let mut write_access = self.data.write().await;
        write_access.blob_created = false;
        write_access.pages.clear();
        return Ok(());
    }

    async fn get(
        &self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        let read_access = self.data.read().await;
        read_access.check_if_blob_exists()?;

        let mut result = Vec::new();

        let mut page_index = start_page_no;

        while page_index < start_page_no + pages_amount {
            result.extend(&read_access.pages[page_index]);

            page_index += 1;
        }

        Ok(result)
    }

    async fn save_pages(
        &self,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let mut write_access = self.data.write().await;
        write_access.check_if_blob_exists()?;

        let pages_amount = payload.len() / BLOB_PAGE_SIZE;
        let mut page_index = start_page_no;

        let mut payload_index = 0;

        while page_index < start_page_no + pages_amount {
            let slice = &payload[payload_index..payload_index + BLOB_PAGE_SIZE];

            let page = write_access.pages.get_mut(page_index).unwrap();

            page.copy_from_slice(slice);

            page_index += 1;
            payload_index += BLOB_PAGE_SIZE;
        }

        Ok(())
    }

    async fn download(&self) -> Result<Vec<u8>, AzureStorageError> {
        let read_access = self.data.read().await;
        read_access.check_if_blob_exists()?;
        return self.get(0, read_access.pages.len()).await;
    }

    async fn get_blob_properties(&self) -> Result<BlobProperties, AzureStorageError> {
        let read_access = self.data.read().await;
        let result = BlobProperties {
            blob_size: read_access.pages.len() * BLOB_PAGE_SIZE,
        };

        Ok(result)
    }
}
