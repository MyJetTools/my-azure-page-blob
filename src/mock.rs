use async_trait::async_trait;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use super::{
    my_azure_page_blob::{get_pages_amount_after_append, get_ressize_to_pages_amount},
    MyPageBlob,
};
pub struct MyPageBlobMock {
    pub pages: Vec<[u8; BLOB_PAGE_SIZE]>,
    pub container_created: bool,
    pub blob_created: bool,
}

impl MyPageBlobMock {
    pub fn new() -> Self {
        Self {
            pages: Vec::new(),
            container_created: false,
            blob_created: false,
        }
    }

    fn add_new_page(&mut self) {
        let new_page = [0u8; BLOB_PAGE_SIZE];
        self.pages.push(new_page);
    }

    fn check_if_container_exists(&self) -> Result<(), AzureStorageError> {
        if self.container_created {
            return Ok(());
        }

        Err(AzureStorageError::ContainerNotFound)
    }

    fn check_if_blob_exists(&self) -> Result<(), AzureStorageError> {
        self.check_if_container_exists()?;

        if self.blob_created {
            return Ok(());
        }

        Err(AzureStorageError::BlobNotFound)
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

    async fn create(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.check_if_container_exists()?;
        self.blob_created = true;

        while self.pages.len() < pages_amount {
            self.add_new_page();
        }
        Ok(())
    }

    async fn create_if_not_exists(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.check_if_container_exists()?;

        self.blob_created = true;

        while self.pages.len() < pages_amount {
            self.add_new_page();
        }
        Ok(())
    }

    async fn get_available_pages_amount(&mut self) -> Result<usize, AzureStorageError> {
        self.check_if_blob_exists()?;
        Ok(self.pages.len())
    }

    async fn create_container_if_not_exist(&mut self) -> Result<(), AzureStorageError> {
        self.container_created = true;
        Ok(())
    }

    async fn resize(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.check_if_blob_exists()?;

        while self.pages.len() < pages_amount {
            self.add_new_page();
        }

        while self.pages.len() > pages_amount {
            self.pages.remove(self.pages.len() - 1);
        }

        Ok(())
    }

    async fn delete(&mut self) -> Result<(), AzureStorageError> {
        self.check_if_blob_exists()?;

        self.blob_created = false;
        return Ok(());
    }

    async fn delete_if_exists(&mut self) -> Result<(), AzureStorageError> {
        self.blob_created = false;
        self.pages.clear();
        return Ok(());
    }

    async fn get(
        &mut self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        self.check_if_blob_exists()?;

        let mut result = Vec::new();

        let mut page_index = start_page_no;

        while page_index < start_page_no + pages_amount {
            result.extend(&self.pages[page_index]);

            page_index += 1;
        }

        Ok(result)
    }

    async fn save_pages(
        &mut self,
        start_page_no: usize,
        payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        self.check_if_blob_exists()?;

        let pages_amount = payload.len() / BLOB_PAGE_SIZE;
        let mut page_index = start_page_no;

        let mut payload_index = 0;

        while page_index < start_page_no + pages_amount {
            let slice = &payload[payload_index..payload_index + BLOB_PAGE_SIZE];

            let page = self.pages.get_mut(page_index).unwrap();

            page.copy_from_slice(slice);

            page_index += 1;
            payload_index += BLOB_PAGE_SIZE;
        }

        Ok(())
    }

    async fn auto_ressize_and_save_pages(
        &mut self,
        start_page_no: usize,
        payload: Vec<u8>,
        resize_pages_ration: usize,
    ) -> Result<(), AzureStorageError> {
        self.check_if_blob_exists()?;
        let pages_amount_after_append = get_pages_amount_after_append(start_page_no, payload.len());

        if pages_amount_after_append > self.pages.len() {
            let pages_amount_needes =
                get_ressize_to_pages_amount(pages_amount_after_append, resize_pages_ration);
            self.resize(pages_amount_needes).await?;
        }

        return self.save_pages(start_page_no, payload).await;
    }

    async fn download(&mut self) -> Result<Vec<u8>, AzureStorageError> {
        self.check_if_blob_exists()?;
        return self.get(0, self.pages.len()).await;
    }
}
