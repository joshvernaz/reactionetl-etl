import file_manager
from database_manager import DatabaseManager

def main():
    
    file_manager.process_incoming_csvs()
    processed_csvs = file_manager.get_processed_csvs()

    dm = DatabaseManager()
    dm.create_tables()
    
    for csv in processed_csvs:
        if dm.validate_schema(file_path=str(csv)):
            dm.ingest_processed_csv(file_path=str(csv))
            if not dm.errored:
                file_manager.move_file_to_ingested(csv)

    metadata_list = file_manager.get_metadata()
    for json in metadata_list:
        dm.ingest_metadata(file_path=str(json))
        if not dm.errored:
            file_manager.move_file_to_ingested(json)

    dm.conn.close()

if __name__ == "__main__":
    main()
