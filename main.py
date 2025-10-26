import file_manager
from database_manager import DatabaseManager
from logging_config import setup_logging
import logging

def main():
    
    setup_logging()    
    logger = logging.getLogger(__name__)
    logger.info("Starting ETL job")

    dm = DatabaseManager()

    # Ingest CSVs
    logger.info("Starting CSV processing")
    file_manager.process_incoming_csvs()
    logger.info("Finished CSV processing")

    processed_csvs = file_manager.get_processed_csvs()
    for csv in processed_csvs:
        simulation_id = str(csv.name)[8:-4]

        if dm.validate_schema(file_path=str(csv)):
            etl_id = dm.insert_etl_run_log(simulation_id=simulation_id, etl_type="reaction")
            inserted_rows = dm.ingest_processed_csv(file_path=str(csv))

            if not dm.errored:
                file_manager.move_file_to_ingested(csv)
                dm.update_etl_run_log(etl_id=etl_id, etl_type="reaction", row_count=inserted_rows)

    # Ingest metadata
    metadata_list = file_manager.get_metadata()
    for json in metadata_list:
        simulation_id = str(json.name)[9:-5]

        etl_id = dm.insert_etl_run_log(simulation_id=simulation_id, etl_type = "metadata")
        dm.ingest_metadata(file_path=str(json))

        if not dm.errored:
            file_manager.move_file_to_ingested(json)
            dm.update_etl_run_log(etl_id=etl_id, etl_type="metadata", row_count=inserted_rows)

    dm.conn.close()

    logger.info("Finished ETL job")

if __name__ == "__main__":
    main()
