from pandas import DataFrame, read_csv
from pathlib import Path
from shutil import move
from typing import List
import json
import logging

# incoming:     Newly finished simulations
# archive:      Simulations that have already been processed
# processed:    Cleaned CSVs from incoming
# ingested:     Cleaned CSVs loaded into PostgreSQL

logger = logging.getLogger(__name__)

def process_incoming_csvs():
    """Cleans CSV files so they can be bulk ingested by PostgreSQL COPY command"""
    logger.info("Starting CSV processing")

    processed_dir = (Path.cwd().parent.parent / "processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    try:
        processed_dir.chmod(mode=0o777)
    except PermissionError:
        logger.warning(f"Could not chmod {str(processed_dir)}")

    sim_dir = Path.cwd().parent.parent / "incoming"
    sim_days = [child for child in sim_dir.iterdir() if child.is_dir()]

    for day in sim_days:
        # Ensure destination and archive directories exists
        destination_dir = (day.parent.parent / "processed" / day.name)
        destination_dir.mkdir(parents=True, exist_ok=True)
        try:
            destination_dir.chmod(mode=0o777)
        except PermissionError:
            logger.warning(f"Could not chmod {str(destination_dir)}")

        archive_dir = (day.parent.parent / "archive" / day.name)
        archive_dir.mkdir(parents=True, exist_ok=True)
        try:
            archive_dir.chmod(mode=0o777)
        except PermissionError:
            logger.warning(f"Could not chmod {str(archive_dir)}")

        for file in [x for x in day.iterdir() if x.suffix == ".csv"]:
            # Store clean CSVs in processed
            df = read_csv(filepath_or_buffer=file, header=0, index_col=False)
            clean_successful = False

            with open("column_map.json", mode="r") as map:
                column_map = json.load(map)
                
            df = df.rename(columns=column_map)

            missing = set(column_map.values()) - set(df.columns)
            if missing:
                logger.error(f"CSV is missing required columns: {missing}")
                raise ValueError(f"CSV is missing required columns: {missing}")

            try:
                if "Unnamed: 0" in df.columns:
                    df = df.drop(columns=["Unnamed: 0"])
                df.to_csv(destination_dir / file.name, index=False, index_label=False)
                logger.info(f"{file.name} successfully processed and written to {destination_dir}")
                clean_successful = True
            except PermissionError as e:
                print(f"{e}")
            except Exception as e:
                print(f"Unknown error - {e}")
            
            # Move source data to archive
            if clean_successful:
                move(src=file, dst=archive_dir)
                logger.info(f"{file.name} moved to {archive_dir}")

    logger.info("Finished CSV processing")

def get_processed_csvs() -> List[Path]:
    """Get a list of processed CSVs not yet ingested"""
    output = []

    # Ensure ingested directory exists
    ingested_dir = (Path.cwd().parent.parent / "ingested")
    ingested_dir.mkdir(parents=True, exist_ok=True)
    try:
        ingested_dir.chmod(mode=0o777)
    except PermissionError:
        logger.warning(f"Could not chmod {str(ingested_dir)}")
        print(f"Warning - could not chmod {str(ingested_dir)}")

    processed_dir = Path.cwd().parent.parent / "processed"
    processed_days = [child for child in processed_dir.iterdir() if child.is_dir()]

    for day in processed_days:
        destination_dir = (day.parent.parent / "ingested" / day.name)
        destination_dir.mkdir(parents=True, exist_ok=True)
        try:
            destination_dir.chmod(mode=0o777)
        except PermissionError:
            logger.warning(f"Could not chmod {str(destination_dir)}")

        for file in [x for x in day.iterdir() if x.suffix == ".csv"]:
            output.append(file)

    return output

def move_file_to_ingested(file: Path):
    """Moves the file to the ingested directory"""
    day_name = file.parent.name
    move(src=file, dst=file.parent.parent.parent / "ingested" / day_name)

def get_metadata() -> List[Path]:
    """Get a list of metadata files of reactions not yet ingested. Should be called AFTER get_processed_csvs(), which creates the ../ingested/ directory"""
    output = []

    incoming_dir = Path.cwd().parent.parent / "incoming"
    days = [child for child in incoming_dir.iterdir() if child.is_dir()]

    for day in days:
        for file in [x for x in day.iterdir() if x.suffix == ".json"]:
            output.append(file)
            
    return output