use my_azure_storage_sdk::{
    blob::BlobApi,
    blob_container::BlobContainersApi,
    page_blob::{consts::BLOB_PAGE_SIZE, PageBlobApi},
    AzureConnection, AzureStorageError,
};

use async_trait::async_trait;

use super::MyPageBlob;

pub struct MyAzurePageBlob {
    pub container_name: String,
    pub blob_name: String,
    pages_available: Option<usize>,
    connection: AzureConnection,
}

impl MyAzurePageBlob {
    pub fn new(connection: AzureConnection, container_name: String, blob_name: String) -> Self {
        Self {
            container_name: container_name,
            blob_name: blob_name,
            pages_available: None,
            connection,
        }
    }

    async fn read_blob_size(&mut self) -> Result<usize, AzureStorageError> {
        let props = self
            .connection
            .get_blob_properties(self.container_name.as_str(), self.blob_name.as_str())
            .await?;

        let result = props.blob_size / BLOB_PAGE_SIZE;
        self.pages_available = Some(result);

        return Ok(result);
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
    async fn create_container_if_not_exist(&mut self) -> Result<(), AzureStorageError> {
        self.connection
            .create_container_if_not_exist(self.container_name.as_str())
            .await
    }

    async fn get_available_pages_amount(&mut self) -> Result<usize, AzureStorageError> {
        match self.pages_available {
            Some(result) => {
                return Ok(result);
            }

            None => {
                return self.read_blob_size().await;
            }
        }
    }

    async fn create(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.connection
            .create_page_blob(self.container_name.as_str(), &self.blob_name, pages_amount)
            .await?;

        self.pages_available = Some(pages_amount);

        return Ok(());
    }

    async fn create_if_not_exists(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        let props = self
            .connection
            .create_page_blob_if_not_exists(
                self.container_name.as_str(),
                &self.blob_name,
                pages_amount,
            )
            .await?;

        let result = props.blob_size / BLOB_PAGE_SIZE;
        self.pages_available = Some(result);

        return Ok(());
    }

    async fn get(
        &mut self,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        self.connection
            .get_pages(
                self.container_name.as_str(),
                self.blob_name.as_str(),
                start_page_no,
                pages_amount,
            )
            .await
    }

    async fn save_pages(
        &mut self,
        start_page_no: usize,
        max_pages_to_write: usize,
        mut payload: Vec<u8>,
    ) -> Result<(), AzureStorageError> {
        let max_write_chunk = BLOB_PAGE_SIZE * max_pages_to_write;

        ressize_payload_to_fullpage(&mut payload);

        let pages_amount_after_append = get_pages_amount_after_append(start_page_no, payload.len());

        let available_pages_amount = self.get_available_pages_amount().await?;

        if pages_amount_after_append > available_pages_amount {
            return Err(AzureStorageError::UnknownError {msg : format!("Can not save pages. Requires blob with the pages amount: {}. Available pages amount is: {}", pages_amount_after_append, available_pages_amount)});
        }

        if payload.len() <= max_write_chunk {
            self.connection
                .save_pages(
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                    start_page_no,
                    payload,
                )
                .await?;

            return Ok(());
        }

        let mut remains_len = payload.len();
        let mut pos = 0;
        let mut start_page_no = start_page_no;

        while remains_len > 0 {
            let mut write_size = remains_len;

            if write_size > max_write_chunk {
                write_size = max_write_chunk;
            }

            let mut chunk = Vec::with_capacity(write_size);

            chunk.extend(&payload[pos..pos + write_size]);

            println!("Debbug: Writing chunk with size {} to blob", chunk.len());

            self.connection
                .save_pages(
                    self.container_name.as_str(),
                    self.blob_name.as_str(),
                    start_page_no,
                    chunk.to_vec(),
                )
                .await?;

            pos += write_size;
            remains_len -= write_size;
            start_page_no += write_size / BLOB_PAGE_SIZE;
        }

        Ok(())
    }

    async fn resize(&mut self, pages_amount: usize) -> Result<(), AzureStorageError> {
        self.connection
            .resize_page_blob(
                self.container_name.as_str(),
                self.blob_name.as_str(),
                pages_amount,
            )
            .await?;

        self.pages_available = Some(pages_amount);

        Ok(())
    }

    async fn auto_ressize_and_save_pages(
        &mut self,
        start_page_no: usize,
        max_pages_to_write_single_round_trip: usize,
        mut payload: Vec<u8>,
        resize_pages_ration: usize,
    ) -> Result<(), AzureStorageError> {
        ressize_payload_to_fullpage(&mut payload);

        let pages_amount_after_append = get_pages_amount_after_append(start_page_no, payload.len());

        let available_pages_amount = self.get_available_pages_amount().await?;

        if self.container_name.contains("bidask") {
            println!(
                "debug: {}/{}. Required size: {}. Current Size: {}",
                self.container_name,
                self.blob_name,
                pages_amount_after_append,
                available_pages_amount
            );
        }

        if pages_amount_after_append > available_pages_amount {
            let pages_amount_needes =
                get_ressize_to_pages_amount(pages_amount_after_append, resize_pages_ration);

            if self.container_name.contains("bidask") {
                println!(
                    "debug: {}/{}. Ressizing to {}",
                    self.container_name, self.blob_name, pages_amount_needes
                );
            }
            self.resize(pages_amount_needes).await?;
        }

        self.save_pages(start_page_no, max_pages_to_write_single_round_trip, payload)
            .await?;

        Ok(())
    }

    async fn delete(&mut self) -> Result<(), AzureStorageError> {
        self.connection
            .delete_blob(self.container_name.as_str(), self.blob_name.as_str())
            .await?;

        self.pages_available = None;
        Ok(())
    }

    async fn delete_if_exists(&mut self) -> Result<(), AzureStorageError> {
        self.connection
            .delete_blob_if_exists(self.container_name.as_str(), self.blob_name.as_str())
            .await?;

        self.pages_available = None;
        Ok(())
    }

    async fn download(&mut self) -> Result<Vec<u8>, AzureStorageError> {
        self.connection
            .download_blob(self.container_name.as_str(), self.blob_name.as_ref())
            .await
    }
}

pub fn get_pages_amount_after_append(start_page_no: usize, data_len: usize) -> usize {
    let data_len_in_pages = data_len / BLOB_PAGE_SIZE;
    return start_page_no + data_len_in_pages;
}

pub fn get_ressize_to_pages_amount(pages_amount_needs: usize, pages_resize_ratio: usize) -> usize {
    let full_pages_amount = (pages_amount_needs - 1) / pages_resize_ratio + 1;

    return full_pages_amount * pages_resize_ratio;
}

fn get_full_pages_size(len: usize) -> usize {
    let pages = (len - 1) / BLOB_PAGE_SIZE;

    (pages + 1) * BLOB_PAGE_SIZE
}

pub fn ressize_payload_to_fullpage(payload: &mut Vec<u8>) {
    let mut remains_to_resize = get_full_pages_size(payload.len()) - payload.len();

    while remains_to_resize > 0 {
        payload.push(0);
        remains_to_resize -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_full_page_ressize() {
        assert_eq!(512, get_full_pages_size(1));
        assert_eq!(512, get_full_pages_size(512));
        assert_eq!(1024, get_full_pages_size(513));
        assert_eq!(1024, get_full_pages_size(1024));
    }

    #[test]
    fn test_ressize_payload_to_full_page() {
        let mut payload: Vec<u8> = Vec::new();

        payload.push(1);

        ressize_payload_to_fullpage(&mut payload);

        assert_eq!(BLOB_PAGE_SIZE, payload.len());
    }

    #[test]
    fn test_get_pages_amount_after_append() {
        assert_eq!(3, get_pages_amount_after_append(2, 512));
    }

    #[test]
    fn test_new_blob_size_in_pages_by_2() {
        let need_pages = 1;
        let pages_ratio = 2;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(2, ressize_to_pages_amount);

        let need_pages = 2;
        let pages_ratio = 2;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(2, ressize_to_pages_amount);

        let need_pages = 3;
        let pages_ratio = 2;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(4, ressize_to_pages_amount);

        let need_pages = 4;
        let pages_ratio = 2;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(4, ressize_to_pages_amount);
    }

    #[test]
    fn test_new_blob_size_in_pages_by_3() {
        let need_pages = 1;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(3, ressize_to_pages_amount);

        let need_pages = 2;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(3, ressize_to_pages_amount);

        let need_pages = 3;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(3, ressize_to_pages_amount);
        //

        let need_pages = 4;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(6, ressize_to_pages_amount);

        let need_pages = 5;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(6, ressize_to_pages_amount);

        let need_pages = 6;
        let pages_ratio = 3;

        let ressize_to_pages_amount = get_ressize_to_pages_amount(need_pages, pages_ratio);

        assert_eq!(6, ressize_to_pages_amount);
    }
}
