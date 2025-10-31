import json
import psycopg2
from pandas import read_csv
from pydantic import BaseModel, Field, ValidationError
import os
from dotenv import load_dotenv
import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

class Metadata(BaseModel):
    simulation_id: str = Field(..., alias="simulation_id")
    reaction_name: str = Field(..., alias="reaction_name")
    activation_energy: float = Field(..., alias="activation_energy (J/mol)")
    ca0: float = Field(..., alias="CA0_(mol/m^3)")
    cb0: float = Field(..., alias="CB0_(mol/m^3)")
    t0: float = Field(..., alias="T0_(K)")
    date_run: str = Field(..., alias="date_run")
    stop_reason: str = Field(..., alias="stop_reason")
    stop_time_s: float = Field(..., alias="stop_time_(s)")

class DatabaseManager:
    def __init__(self):
        load_dotenv()
        logger.info(f"Successfully connected to {os.getenv('DB_NAME')}")
        try:
            self.conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"), 
                user=os.getenv("DB_USER"), 
                password=os.getenv("DB_PASS")
                )
        except Exception as e:
            logger.exception(f"Failed to create connection to {os.getenv("DB_NAME")}", exc_info=True)
        
        self.create_tables()
        self.create_indexes()

    def validate_schema(self, file_path: str):
        """
        Checks if the schema of processed CSV differs from current state of database.
        Takes PROCESSED CSV
        """
        file_path_Path = Path(file_path)
        df = read_csv(file_path)
        incoming_schema = set(df.columns.to_list())

        with open("required_cols.json", mode="r") as f:
            cols = json.load(f)
            required_cols = [value for key, value in cols.items()]

        if set(required_cols) - incoming_schema:
            raise ValueError("Required columns are not present")
        
        logger.info(f"Schema for {file_path_Path.name[8:-4]} validated")
        return True

    def create_tables(self):
        with open(file="create_tables.sql", mode="r") as command:
            sql = command.read()
            with self.conn.cursor() as cur:
                try:
                    cur.execute(sql)
                    self.conn.commit()
                except:
                    self.conn.rollback()

    def create_indexes(self):
        with open(file="create_indexes.sql", mode="r") as command:
            sql = command.read()
            with self.conn.cursor() as cur:
                try:
                    cur.execute(sql)
                    self.conn.commit()
                except:
                    self.conn.rollback()
                
    def ingest_processed_csv(self, file_path) -> int:
        """
        Ingests the processed CSV file. Returns the number of rows inserted into the table fact_sim.
        """
        file_path_Path = Path(file_path)

        logger.info(f"Starting data ingestion for {file_path_Path.name[8:-4]}")
        df = read_csv(file_path)
        cols = df.columns.to_list()
        sql = f"copy fact_sim ({", ".join(cols)}) from '{file_path}' with (format csv, header match);"

        with self.conn.cursor() as cur:
            try:
                cur.execute(sql)
                rows_inserted = cur.rowcount
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                self.errored = True
                logger.exception(f"Failed to ingest {file_path_Path.name}", exc_info=True)
            finally:
                cur.close()

        self.errored = False
        logger.info(f"Finished data ingestion for {file_path_Path.name[8:-4]} with {rows_inserted} rows inserted")

        return rows_inserted

    def ingest_metadata(self, file_path):        
        file_path_Path = Path(file_path)
        logger.info(f"Starting metadata ingestion for {file_path_Path.name[9:-5]}")
        with open(file=file_path, mode="r") as f:
            file = json.load(f)

        try:
            metadata = Metadata(**file)
        except ValidationError as e:
            logger.warning(f"Failed to validate metadata for {file_path_Path.name}")
        
        inserts = metadata.model_dump()
        
        cols = [key for key, value in inserts.items()]
        values = [value for key, value in inserts.items()]
        values_as_str = [v if not isinstance(v, (int, float)) else str(v) for v in values]

        replacements = ", ".join(["%s"] * len(values_as_str))
        sql = f"insert into dim_rxn({', '.join(cols)}) values ({replacements})"

        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, values_as_str)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                self.errored = True
                logger.exception(f"Failed to ingest {file_path_Path.name}", exc_info=True)

            finally:
                cur.close()
        
        self.errored = False
        logger.info(f"Finished metadata ingestion for {file_path_Path.name[9:-5]}")

    def insert_etl_run_log(self, simulation_id: str, etl_type: str) -> str:
        """
        Inserts a row into the etl_run_log table for a simulation defined by simulation_id
        etl_type should be 'metadata' or 'reaction'
        """
        with self.conn.cursor() as cur:
            
            try:
                cur.execute("""
                            insert into etl_run_log (simulation_id, etl_type, status)
                            values (%s, %s, 'running')
                            returning etl_id
                            """, (simulation_id, etl_type))
                etl_id = cur.fetchone()[0]
                self.conn.commit()

            except Exception as e:
                logger.error(f"Failed to insert row into etl_run_log for simulation_id {simulation_id}")
                self.conn.rollback()
                self.errored = True

            finally:
                cur.close()

        self.errored = False
        
        return etl_id
    
    def update_etl_run_log(self, etl_id: str, etl_type: str, row_count: int):
        """
        Updates the etl_run_log table upon successful ingestion
        etl_type should be 'metadata' or 'reaction'
        """
        with self.conn.cursor() as cur:

            try:
                cur.execute("""
                            update etl_run_log
                            set finished_at = now(),
                                records_inserted = %s,
                                status = 'success',
                                duration_seconds = extract(epoch from (now() - started_at))
                            where
                                etl_id = %s
                                and etl_type = %s
                            """, (row_count, etl_id, etl_type)
                )
                self.conn.commit()
            
            except Exception as e:
                logger.error(f"Failed to update row in etl_run_log for etl_id {etl_id}")
                self.conn.rollback()
                self.errored = True

            finally:
                cur.close()

    def update_simulation_num(self, simulation_id):
        """
        Updates the fact_sim table with the matching simulation_num from dim_rxn
        """
        with self.conn.cursor() as cur:
            try:
                cur.execute("""
                            update fact_sim fs
                            set simulation_num = dr.simulation_num
                            from dim_rxn dr
                            where 
                                fs.simulation_id = dr.simulation_id
                                and dr.simulation_id = %s;
                            """, (simulation_id,))
                self.conn.commit()
                
            except:
                logger.error(f"Failed to update fact_sim.simulation_num for simulation_id {simulation_id}")
                self.conn.rollback()
                self.errored = True
            finally:
                cur.close()

    def get_indexes(self) -> List[str]:
        with self.conn.cursor() as cur:
            try:
                cur.execute("select indexname from pg_indexes where tablename not like 'pg%' and indexdef not like 'create unique%' and indexname like 'idx%';")
                self.conn.commit()
                index_list = cur.fetchall()
            except:
                self.conn.rollback()
        
        indexes = []
        for i in range(len(index_list)):
            indexes.append(index_list[i][0])

        return indexes
    
    def drop_indexes(self):
        indexes = self.get_indexes()
        
        with self.conn.cursor() as cur:
            for index in indexes:
                try:
                    cur.execute(f"""drop index {index};""")
                    self.conn.commit()
                    logger.info(f"Dropped index {index}")
                except Exception as e:
                    logger.error(f"Failed to drop index {index}: {e}")

    # def get_index_defs(self) -> List[str]:
    #     """Returns a list of CREATE INDEX statements for the indexes before """
    #     with self.conn.cursor() as cur:
    #         try:
    #             cur.execute("select indexdef from pg_indexes where tablename not like 'pg_%' and indexdef not like 'create unique%';")
    #             self.conn.commit()
    #             index_list = cur.fetchall()
    #         except:
    #             self.conn.rollback()

    #     index_defs = []
    #     for i in range(len(index_list)):
    #         index_defs.append(index_list[i])

    #     return index_defs

    def recreate_indexes(self):
        self.create_indexes()

