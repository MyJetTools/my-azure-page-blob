use std::sync::Arc;

use my_azure_storage_sdk::{
    page_blob::consts::BLOB_PAGE_SIZE, AzureConnectionInfo, AzureStorageError,
};
use my_telemetry::MyTelemetry;

pub struct MyAzurePageBlobSdk {
    pub container_name: String,
    pub blob_name: String,
    pages_available: Option<usize>,
}

impl MyAzurePageBlobSdk {
    pub fn new(container_name: String, blob_name: String) -> Self {
        Self {
            container_name,
            blob_name,
            pages_available: None,
        }
    }
    #[inline]
    pub async fn resize<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::page_blob::sdk::resize_page_blob(
            connection,
            self.container_name.as_str(),
            self.blob_name.as_str(),
            pages_amount,
            my_telemetry,
        )
        .await?;

        self.pages_available = Some(pages_amount);

        Ok(())
    }
    #[inline]
    pub async fn create_container_if_not_exist<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
    ) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::blob_container::sdk::create_container_if_not_exist(
            connection,
            self.container_name.as_str(),
            my_telemetry,
        )
        .await?;

        Ok(())
    }
    #[inline]
    pub async fn read_blob_size<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
    ) -> Result<usize, AzureStorageError> {
        let props = my_azure_storage_sdk::blob::sdk::get_blob_properties(
            &connection,
            self.container_name.as_str(),
            self.blob_name.as_str(),
            my_telemetry,
        )
        .await?;

        let result = props.blob_size / BLOB_PAGE_SIZE;

        self.pages_available = Some(result);

        return Ok(result);
    }
    #[inline]
    pub async fn get_available_pages_amount<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
    ) -> Result<usize, AzureStorageError> {
        match self.pages_available {
            Some(result) => {
                return Ok(result);
            }

            None => {
                return self.read_blob_size(connection, my_telemetry).await;
            }
        }
    }
    #[inline]
    pub async fn create<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::page_blob::sdk::create_page_blob(
            connection,
            self.container_name.as_str(),
            &self.blob_name,
            pages_amount,
            my_telemetry,
        )
        .await?;

        self.pages_available = Some(pages_amount);

        return Ok(());
    }
    #[inline]
    pub async fn create_if_not_exists<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
        pages_amount: usize,
    ) -> Result<(), AzureStorageError> {
        let props = my_azure_storage_sdk::page_blob::sdk::create_page_blob_if_not_exists(
            connection,
            self.container_name.as_str(),
            &self.blob_name,
            pages_amount,
            my_telemetry,
        )
        .await?;

        let result = props.blob_size / BLOB_PAGE_SIZE;
        self.pages_available = Some(result);

        return Ok(());
    }

    pub async fn get<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
        start_page_no: usize,
        pages_amount: usize,
    ) -> Result<Vec<u8>, AzureStorageError> {
        my_azure_storage_sdk::page_blob::sdk::get_pages(
            connection,
            self.container_name.as_str(),
            self.blob_name.as_str(),
            start_page_no,
            pages_amount,
            my_telemetry,
        )
        .await
    }
    #[inline]
    pub async fn save_pages<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
        start_page_no: usize,
        max_pages_to_write: usize,
        mut payload: Vec<u8>,
    ) -> Result<usize, AzureStorageError> {
        let max_write_chunk = BLOB_PAGE_SIZE * max_pages_to_write;

        ressize_payload_to_fullpage(&mut payload);

        let result = payload.len();

        let pages_amount_after_append = get_pages_amount_after_append(start_page_no, payload.len());

        let available_pages_amount = self
            .get_available_pages_amount(connection, my_telemetry.clone())
            .await?;

        if pages_amount_after_append > available_pages_amount {
            return Err(AzureStorageError::UnknownError {msg : format!("Can not save pages. Requires blob with the pages amount: {}. Available pages amount is: {}", pages_amount_after_append, available_pages_amount)});
        }

        if payload.len() <= max_write_chunk {
            my_azure_storage_sdk::page_blob::sdk::save_pages(
                connection,
                self.container_name.as_str(),
                self.blob_name.as_str(),
                start_page_no,
                payload,
                my_telemetry,
            )
            .await?;

            return Ok(result);
        }

        let mut remains_len = payload.len();
        let mut pos = 0;
        let mut start_page_no = start_page_no;

        while remains_len > 0 {
            let write_amount = if remains_len > max_write_chunk {
                max_write_chunk
            } else {
                remains_len
            };

            let payload_to_write = &payload[pos..pos + write_amount];

            println!(
                "Debbug: {}/{} Writing chunk to the page {} with size {} to blob",
                self.container_name.as_str(),
                self.blob_name.as_str(),
                start_page_no,
                payload_to_write.len()
            );

            my_azure_storage_sdk::page_blob::sdk::save_pages(
                connection,
                self.container_name.as_str(),
                self.blob_name.as_str(),
                start_page_no,
                payload_to_write.to_vec(),
                my_telemetry.clone(),
            )
            .await?;

            pos += write_amount;
            remains_len -= write_amount;
            start_page_no += write_amount / BLOB_PAGE_SIZE;
        }

        Ok(result)
    }
    #[inline]
    pub async fn auto_ressize_and_save_pages<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
        start_page_no: usize,
        max_pages_to_write_single_round_trip: usize,
        mut payload: Vec<u8>,
        resize_pages_ration: usize,
    ) -> Result<usize, AzureStorageError> {
        ressize_payload_to_fullpage(&mut payload);

        let pages_amount_after_append = get_pages_amount_after_append(start_page_no, payload.len());

        let available_pages_amount = self
            .get_available_pages_amount(connection, my_telemetry.clone())
            .await?;

        if pages_amount_after_append > available_pages_amount {
            let pages_amount_needes =
                get_ressize_to_pages_amount(pages_amount_after_append, resize_pages_ration);

            self.resize(connection, my_telemetry.clone(), pages_amount_needes)
                .await?;
        }

        let result = self
            .save_pages(
                connection,
                my_telemetry.clone(),
                start_page_no,
                max_pages_to_write_single_round_trip,
                payload,
            )
            .await?;

        return Ok(result);
    }
    #[inline]
    pub async fn delete<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
    ) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::blob::sdk::delete_blob(
            connection,
            self.container_name.as_str(),
            self.blob_name.as_str(),
            my_telemetry,
        )
        .await?;

        self.pages_available = None;
        Ok(())
    }
    #[inline]
    pub async fn delete_if_exists<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
    ) -> Result<(), AzureStorageError> {
        my_azure_storage_sdk::blob::sdk::delete_blob_if_exists(
            connection,
            self.container_name.as_str(),
            self.blob_name.as_str(),
            my_telemetry,
        )
        .await?;

        self.pages_available = None;
        Ok(())
    }

    #[inline]
    pub async fn download<'s, TMyTelemetry: MyTelemetry>(
        &mut self,
        connection: &AzureConnectionInfo,
        my_telemetry: Option<Arc<TMyTelemetry>>,
    ) -> Result<Vec<u8>, AzureStorageError> {
        my_azure_storage_sdk::blob::sdk::download_blob(
            connection,
            self.container_name.as_str(),
            self.blob_name.as_str(),
            my_telemetry,
        )
        .await
    }
}

pub fn ressize_payload_to_fullpage(payload: &mut Vec<u8>) {
    let mut remains_to_resize = get_full_pages_size(payload.len()) - payload.len();

    while remains_to_resize > 0 {
        payload.push(0);
        remains_to_resize -= 1;
    }
}

fn get_full_pages_size(len: usize) -> usize {
    let pages = (len - 1) / BLOB_PAGE_SIZE;

    (pages + 1) * BLOB_PAGE_SIZE
}

pub fn get_pages_amount_after_append(start_page_no: usize, data_len: usize) -> usize {
    let data_len_in_pages = data_len / BLOB_PAGE_SIZE;
    return start_page_no + data_len_in_pages;
}

pub fn get_ressize_to_pages_amount(pages_amount_needs: usize, pages_resize_ratio: usize) -> usize {
    let full_pages_amount = (pages_amount_needs - 1) / pages_resize_ratio + 1;

    return full_pages_amount * pages_resize_ratio;
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
