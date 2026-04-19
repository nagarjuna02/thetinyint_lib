import logging, time
from datetime import datetime
from azure.storage.blob import ContainerClient

class Logger:
    _logger = None

    @classmethod
    def setup(cls, sas_url, blob_path, app_name="thetinyint_app"):
        if cls._logger:
            return cls._logger

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Azure Handler
        azure_handler = AzureBlobLoggingHandler(sas_url, blob_path, log_prefix=app_name)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        azure_handler.setFormatter(formatter)

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        root_logger.addHandler(azure_handler)
        root_logger.addHandler(console_handler)
        root_logger.setLevel(logging.INFO)
        
        logging.getLogger("azure").setLevel(logging.WARNING)

        cls._logger = logging.getLogger(app_name)
        cls._logger.info(f"Cloud Logging Initialized for {app_name}")
        return cls._logger

   
class AzureBlobLoggingHandler(logging.Handler):
    def __init__(self, sas_url, blob_path, log_prefix="log", batch_size=20, flush_interval=60):
        super().__init__()
        self.sas_url = sas_url
        self.blob_path = blob_path
        self.log_prefix = log_prefix
        
        # Batching settings
        self.batch_size = batch_size
        self.flush_interval = flush_interval # seconds
        self.last_flush_time = time.time()
        
        self.container_client = ContainerClient.from_container_url(self.sas_url)
        self.buffer = []

    def emit(self, record):
        try:
            log_entry = self.format(record) + "\n"
            self.buffer.append(log_entry)
            
            # Flush if buffer is full OR if enough time has passed
            current_time = time.time()
            time_since_last = current_time - self.last_flush_time
            
            if len(self.buffer) >= self.batch_size or time_since_last >= self.flush_interval:
                self.flush()
        except Exception as e:
            print(f"Logging emit error: {e}")

    def flush(self):
        if not self.buffer:
            return
        
        today_str = datetime.now().strftime('%Y%m%d')
        blob_name = f"{self.blob_path}/{self.log_prefix}_{today_str}.log"
        
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            
            # Check for existing data to append
            try:
                # Note: In a high-concurrency environment, you'd use Append Blobs, 
                # but for single-agent scripts, Block Blob overwrite is fine.
                existing_data = blob_client.download_blob().readall().decode('utf-8')
            except Exception:
                existing_data = ""

            new_data = existing_data + "".join(self.buffer)
            blob_client.upload_blob(new_data, overwrite=True)
            
            # Reset state
            self.buffer = [] 
            self.last_flush_time = time.time()
        except Exception as e:
            print(f"Failed to write logs to blob: {e}")

    def close(self):
        """Ensure remaining logs are sent when the script finishes."""
        self.flush()
        super().close()